package redislock_test

import (
	"context"
	"crypto/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hxy2321548628/redislock"
)

// These benchmarks include real Redis round trips. Set REDIS_ADDR to run them.
func BenchmarkRedisAcquireRelease(b *testing.B) {
	address := os.Getenv("REDIS_ADDR")
	if address == "" {
		b.Skip("set REDIS_ADDR to benchmark Redis")
	}
	backend := integrationBackend(b, address)
	b.Run("Uncontended", func(b *testing.B) {
		lock, err := redislock.NewLock("benchmark:"+rand.Text(), backend)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			lease, err := lock.TryAcquire(b.Context())
			if err != nil {
				b.Fatal(err)
			}
			if err := lease.Release(b.Context()); err != nil {
				b.Fatal(err)
			}
		}
	})
	for _, name := range []string{"Contended", "IndependentKeys"} {
		b.Run(name, func(b *testing.B) {
			key := "benchmark:" + rand.Text()
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				workerKey := key
				if name == "IndependentKeys" {
					workerKey += ":" + rand.Text()
				}
				lock, err := redislock.NewLock(workerKey, backend, redislock.WithRetryInterval(time.Millisecond))
				if err != nil {
					b.Error(err)
					return
				}
				for pb.Next() {
					lease, err := lock.Acquire(b.Context())
					if err != nil {
						b.Error(err)
						return
					}
					if err := lease.Release(b.Context()); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

func BenchmarkRedlockAcquireRelease(b *testing.B) {
	addresses := os.Getenv("REDIS_ADDRS")
	if addresses == "" {
		b.Skip("set REDIS_ADDRS to benchmark independent Redis nodes")
	}
	var backends []redislock.Backend
	for _, address := range strings.Split(addresses, ",") {
		backends = append(backends, integrationBackend(b, strings.TrimSpace(address)))
	}
	lock, err := redislock.NewRedlock("benchmark:"+rand.Text(), backends, 5*time.Second, 200*time.Millisecond)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		lease, err := lock.TryAcquire(b.Context())
		if err != nil {
			b.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err = lease.Release(ctx)
		cancel()
		if err != nil {
			b.Fatal(err)
		}
	}
}
