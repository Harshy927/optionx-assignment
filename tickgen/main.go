// tickgen streams a deterministic, bursty feed of option ticks over TCP as
// newline-delimited JSON. Every run with the same -seed produces the identical
// tick stream (content, seq numbers, prices) regardless of timing, so a
// consumer can be killed, restarted, and replayed from any point with -from.
//
// Usage:
//
//	go run . -addr :9001                 # serve the stream
//	go run . -addr :9001 -from 150000    # serve, starting at global event 150000
//	nc localhost 9001 | head             # peek at the stream
//
// Each connected client independently receives the stream from -from at the
// paced (bursty) rate. Timestamps are wall-clock at emit time and are the only
// non-deterministic field.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"time"
)

type instrument struct {
	token string
	price float64 // current LTP, mutated by the walk
}

const tickSize = 0.05

func buildInstruments(rng *rand.Rand) []*instrument {
	underlyings := []struct {
		name   string
		strike int
		step   int
		base   float64
	}{
		{"NIFTY26AUG", 24500, 100, 180},
		{"BANKNIFTY26AUG", 55000, 500, 420},
	}
	var out []*instrument
	for _, u := range underlyings {
		for i := 0; i < 5; i++ {
			strike := u.strike + i*u.step
			for _, side := range []string{"CE", "PE"} {
				// Deterministic starting price per instrument.
				start := u.base * (0.4 + 1.2*rng.Float64())
				out = append(out, &instrument{
					token: fmt.Sprintf("%s%d%s", u.name, strike, side),
					price: roundTick(start),
				})
			}
		}
	}
	return out // 2 underlyings x 5 strikes x CE/PE = 20 instruments
}

func roundTick(p float64) float64 {
	p = math.Round(p/tickSize) * tickSize
	if p < tickSize {
		p = tickSize
	}
	return math.Round(p*100) / 100
}

// event is one tick's deterministic content (everything except ts).
type event struct {
	seq   int64 // per-instrument sequence
	token string
	ltp   float64
}

// stream regenerates the deterministic event sequence from the beginning and
// invokes emit for every event with global index >= from. It returns only when
// emit returns false (client gone).
func stream(seed int64, from int64, emit func(globalIdx int64, ev event) bool) {
	rng := rand.New(rand.NewSource(seed))
	instruments := buildInstruments(rng)
	seqs := make([]int64, len(instruments))

	for globalIdx := int64(0); ; globalIdx++ {
		i := rng.Intn(len(instruments))
		ins := instruments[i]
		// Small random walk, occasionally a jumpier move.
		move := (rng.Float64() - 0.5) * 2 * tickSize * 3
		if rng.Float64() < 0.02 {
			move *= 8
		}
		ins.price = roundTick(ins.price + move)
		seqs[i]++
		if globalIdx >= from {
			if !emit(globalIdx, event{seq: seqs[i], token: ins.token, ltp: ins.price}) {
				return
			}
		}
	}
}

// pace returns the delay before the next tick, alternating calm and burst
// phases. Pacing uses its own RNG so it never affects stream content.
type pacer struct {
	rng       *rand.Rand
	remaining int
	delay     time.Duration
}

func (p *pacer) next() time.Duration {
	if p.remaining == 0 {
		if p.rng.Float64() < 0.3 {
			// Burst: ~500 ticks/sec for 1-2s.
			p.remaining = 500 + p.rng.Intn(500)
			p.delay = 2 * time.Millisecond
		} else {
			// Calm: 20-50 ticks/sec for a few seconds.
			p.remaining = 60 + p.rng.Intn(90)
			p.delay = time.Duration(20+p.rng.Intn(30)) * time.Millisecond
		}
	}
	p.remaining--
	return p.delay
}

func main() {
	addr := flag.String("addr", ":9001", "TCP address to serve the tick stream on")
	seed := flag.Int64("seed", 42, "stream seed; same seed = identical stream")
	from := flag.Int64("from", 0, "global event index to start streaming from (replay)")
	flag.Parse()

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}
	log.Printf("tickgen serving on %s (seed=%d, from=%d)", *addr, *seed, *from)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatalf("accept: %v", err)
		}
		go func(c net.Conn) {
			defer c.Close()
			log.Printf("client connected: %s", c.RemoteAddr())
			w := bufio.NewWriter(c)
			p := &pacer{rng: rand.New(rand.NewSource(*seed + 1))}
			stream(*seed, *from, func(_ int64, ev event) bool {
				line := fmt.Sprintf(`{"seq": %d, "token": %q, "ltp": %.2f, "ts": %d}`+"\n",
					ev.seq, ev.token, ev.ltp, time.Now().UnixMilli())
				if _, err := w.WriteString(line); err != nil {
					return false
				}
				if err := w.Flush(); err != nil {
					return false
				}
				time.Sleep(p.next())
				return true
			})
			log.Printf("client disconnected: %s", c.RemoteAddr())
		}(conn)
	}
}
