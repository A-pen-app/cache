package cache

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func setupRedis(t *testing.T, poolSize int) *miniredis.Miniredis {
	t.Helper()
	mr := miniredis.RunT(t)
	Initialize(&Config{Prefix: "test", Type: TypeRedis, RedisURL: mr.Addr(), PoolSize: poolSize})
	t.Cleanup(Finalize)
	return mr
}

func TestInitializePoolSize(t *testing.T) {
	setupRedis(t, 42)
	if got := redisClient.Options().PoolSize; got != 42 {
		t.Fatalf("PoolSize = %d, want 42", got)
	}
}

func TestInitializeDefaultPoolSize(t *testing.T) {
	setupRedis(t, 0)
	if got := redisClient.Options().PoolSize; got <= 0 {
		t.Fatalf("PoolSize = %d, want go-redis default > 0", got)
	}
}

func TestMSetWithTTLRedis(t *testing.T) {
	mr := setupRedis(t, 0)
	ctx := context.Background()

	values := map[string]interface{}{
		"a": []string{"x", "y"},
		"b": []string{"z"},
	}
	if err := MSetWithTTL(ctx, values, time.Minute); err != nil {
		t.Fatalf("MSetWithTTL: %v", err)
	}

	var a []string
	if err := Get(ctx, "a", &a); err != nil {
		t.Fatalf("Get a: %v", err)
	}
	if len(a) != 2 || a[0] != "x" || a[1] != "y" {
		t.Fatalf("Get a = %v", a)
	}

	got, err := MGet[[]string](ctx, []string{"a", "b", "missing"})
	if err != nil {
		t.Fatalf("MGet: %v", err)
	}
	if len(got[0]) != 2 || len(got[1]) != 1 || got[1][0] != "z" || got[2] != nil {
		t.Fatalf("MGet = %#v", got)
	}

	for _, key := range []string{"test:a", "test:b"} {
		if ttl := mr.TTL(key); ttl != time.Minute {
			t.Fatalf("TTL(%s) = %v, want 1m", key, ttl)
		}
	}
}

func TestMSetWithTTLEmpty(t *testing.T) {
	setupRedis(t, 0)
	if err := MSetWithTTL(context.Background(), nil, time.Minute); err != nil {
		t.Fatalf("MSetWithTTL(nil): %v", err)
	}
}

func TestMSetWithTTLLocal(t *testing.T) {
	Initialize(&Config{Prefix: "test", Type: TypeLocal})
	ctx := context.Background()

	if err := MSetWithTTL(ctx, map[string]interface{}{"a": []string{"x"}, "b": []string{"y", "z"}}, time.Minute); err != nil {
		t.Fatalf("MSetWithTTL: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		got, err := MGet[[]string](ctx, []string{"a", "b"})
		if err != nil {
			t.Fatalf("MGet: %v", err)
		}
		if len(got[0]) == 1 && got[0][0] == "x" && len(got[1]) == 2 && got[1][1] == "z" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("MGet = %#v, want both keys written", got)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
