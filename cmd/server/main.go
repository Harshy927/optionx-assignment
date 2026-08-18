// Command server is the entrypoint for the OptionX position engine. It wires
// together the Postgres connection, applies migrations, and starts the HTTP
// server (REST API + health check). Later tasks add the tick feed consumer,
// instrument actors, and WebSocket hub here.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/optionx/backend-assignment/internal/candle"
	"github.com/optionx/backend-assignment/internal/feed"
	"github.com/optionx/backend-assignment/internal/httpapi"
	"github.com/optionx/backend-assignment/internal/instrument"
	"github.com/optionx/backend-assignment/internal/storage"
	"github.com/optionx/backend-assignment/internal/ws"
)

func main() {
	cfg := storage.ConfigFromEnv()

	db, err := storage.Open(cfg)
	if err != nil {
		log.Fatalf("failed to connect to postgres: %v", err)
	}
	defer db.Close()
	log.Printf("connected to postgres database %q on %s:%d", cfg.DBName, cfg.Host, cfg.Port)

	migCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := storage.Migrate(migCtx, db, "migrations"); err != nil {
		log.Fatalf("failed to apply migrations: %v", err)
	}
	log.Println("migrations applied")

	// appCtx governs the lifetime of every background goroutine: the feed
	// consumer, the tick-processing loop, and every instrument actor the
	// registry spawns. Cancelling it (on shutdown, below) stops all of them.
	appCtx, appCancel := context.WithCancel(context.Background())
	defer appCancel()

	// Instrument actor registry: one actor per instrument token, created
	// lazily on first order/tick. Actors write through to Postgres (orders +
	// positions) and are seeded from whatever state was persisted before
	// this process started, so a restart followed by a feed replay resumes
	// exactly where the previous process left off -- a resting order stays
	// resting, an already-filled order is never re-evaluated (see
	// instrument.actorState.handleTick, which only matches orders still in
	// StatusResting).
	instrumentStore := storage.NewOrderStore(db)
	seedCtx2, seedCancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	instrumentSeeds, err := storage.LoadAllInstrumentState(seedCtx2, db)
	seedCancel2()
	if err != nil {
		log.Fatalf("failed to load instrument state: %v", err)
	}
	reg := instrument.NewPersistentRegistry(appCtx, instrumentStore, instrumentSeeds)
	log.Printf("seeded instrument registry with %d instrument(s) from postgres", len(instrumentSeeds))

	// WebSocket hub: fans out position/P&L updates to subscribed clients,
	// throttled to one update per instrument per 100ms per client, with a
	// slow/stalled client unable to backpressure the tick loop or other
	// clients (see internal/ws package doc for the full design). Every
	// actor the registry creates from this point on streams into it.
	hub := ws.NewHub()
	reg.SetPublisher(hub)

	addr := ":8080"
	if v := os.Getenv("OPTIONX_HTTP_ADDR"); v != "" {
		addr = v
	}

	srv := &http.Server{
		Addr:    addr,
		Handler: httpapi.NewRouter(db, reg, hub),
	}

	go func() {
		log.Printf("http server listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server error: %v", err)
		}
	}()

	// Feed consumer: connect to tickgen, aggregate ticks into 1-minute OHLC
	// candles (persisted transactionally, see Task 4) AND deliver the same
	// ticks to each instrument's actor so resting limit orders can trigger
	// and positions mark-to-market. The Aggregator is seeded from Postgres
	// on boot (below) so a restart followed by a feed replay resumes
	// exactly where the previous process left off.
	agg := candle.NewAggregator()
	seedCtx, seedCancel := context.WithTimeout(context.Background(), 10*time.Second)
	watermarks, err := storage.AllWatermarks(seedCtx, db)
	if err != nil {
		seedCancel()
		log.Fatalf("failed to load watermarks: %v", err)
	}
	latestCandles, err := storage.AllLatestCandles(seedCtx, db)
	seedCancel()
	if err != nil {
		log.Fatalf("failed to load latest candles: %v", err)
	}
	for token, lastSeq := range watermarks {
		if c, ok := latestCandles[token]; ok {
			agg.Seed(token, lastSeq, &c)
		} else {
			agg.Seed(token, lastSeq, nil)
		}
	}
	log.Printf("seeded aggregator with %d instrument watermark(s) from postgres", len(watermarks))

	feedAddr := "localhost:9001"
	if v := os.Getenv("OPTIONX_FEED_ADDR"); v != "" {
		feedAddr = v
	}
	ticks := make(chan feed.Tick, 1024)
	go func() {
		consumer := feed.NewConsumer(feedAddr)
		log.Printf("connecting to tick feed at %s", feedAddr)
		if err := consumer.Run(appCtx, ticks); err != nil && appCtx.Err() == nil {
			log.Printf("feed consumer stopped: %v", err)
		}
	}()
	go func() {
		for {
			select {
			case t, ok := <-ticks:
				if !ok {
					return
				}
				result := agg.Apply(t)
				dup, err := storage.ApplyTick(appCtx, db, t.Token, t.Seq, result)
				if err != nil {
					log.Printf("failed to persist tick for %s seq=%d: %v", t.Token, t.Seq, err)
					continue
				}

				// Deliver the tick to the instrument's actor regardless of
				// candle-dedup outcome: the actor has its own notion of
				// "latest price" and simply updating it again with the same
				// LTP on a replayed tick is harmless (it does not re-fill
				// already-filled or already-cancelled orders -- see
				// instrument.actorState.handleTick).
				if err := reg.Actor(t.Token).Tick(appCtx, t.LTP); err != nil && appCtx.Err() == nil {
					log.Printf("failed to deliver tick to actor for %s: %v", t.Token, err)
				}

				if dup {
					log.Printf("tick: token=%s seq=%d ltp=%.2f (duplicate, skipped)", t.Token, t.Seq, t.LTP)
					continue
				}
				log.Printf("tick: token=%s seq=%d ltp=%.2f candle_close=%.2f tick_count=%d",
					t.Token, t.Seq, t.LTP, result.Current.Close, result.Current.TickCount)
			case <-appCtx.Done():
				return
			}
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Println("shutting down...")
	appCancel()
	ctx, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
	}
}
