// Package cache abstracts localcache and redis in a same interface
package cache

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/eko/gocache/lib/v4/cache"
	"github.com/eko/gocache/lib/v4/store"
	redis_store "github.com/eko/gocache/store/redis/v4"
	ristretto_store "github.com/eko/gocache/store/ristretto/v4"
	"github.com/redis/go-redis/v9"
)

var cacheManager *cache.Cache[string]
var keyPrefix string
var cacheType Type
var redisClient *redis.Client

const defaultPrefix = "default"

// Type defines all available cache types
type Type int

const (
	TypeLocal Type = iota
	TypeRedis
)

type Config struct {
	Prefix   string
	Type     Type
	RedisURL string
}

func Initialize(c *Config) {
	if c == nil {
		c = &Config{
			Prefix: defaultPrefix,
		}
	}

	var cacheStore store.StoreInterface

	switch c.Type {
	case TypeLocal:
		// TODO let user customize parameters
		ristrettoCache, err := ristretto.NewCache(&ristretto.Config{
			NumCounters: 1e6,
			MaxCost:     1 << 30,
			BufferItems: 64,
		})
		if err != nil {
			panic(err)
		}
		cacheStore = ristretto_store.NewRistretto(ristrettoCache)
	case TypeRedis:
		if len(c.RedisURL) == 0 {
			panic(fmt.Errorf("initialization with redis needs redis url"))
		}
		redisClient = redis.NewClient(&redis.Options{
			Addr: c.RedisURL,
		})
		cacheStore = redis_store.NewRedis(redisClient)
	default:
		panic(fmt.Errorf("unknown cache type"))

	}
	cacheManager = cache.New[string](cacheStore)
	keyPrefix = c.Prefix
	cacheType = c.Type
}

func Finalize() {
	if redisClient != nil {
		redisClient.Close()
		redisClient = nil
	}
}

func compose(key string) string {
	return keyPrefix + ":" + key
}

func marshal(val interface{}) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(val); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func unmarshal(data []byte, container interface{}) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode(container)
}

func Set(ctx context.Context, key string, value interface{}) error {
	data, err := marshal(value)
	if err != nil {
		return err
	}
	return cacheManager.Set(ctx, compose(key), string(data))
}

func SetWithTTL(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	data, err := marshal(value)
	if err != nil {
		return err
	}
	return cacheManager.Set(ctx, compose(key), string(data), store.WithExpiration(ttl))
}

func Get(ctx context.Context, key string, container interface{}) error {
	data, err := cacheManager.Get(ctx, compose(key))
	if err != nil && err.Error() == store.NOT_FOUND_ERR {
		return ErrorNotFound
	} else if err != nil {
		return err
	}
	return unmarshal([]byte(data), container)
}

func Delete(ctx context.Context, key string) error {
	return cacheManager.Delete(ctx, compose(key))
}

// MGet retrieves multiple values of the same type from the cache in a single operation.
// For Redis backend, this uses a single MGET call for efficiency.
// For local cache, this falls back to sequential Get calls.
// Returns a slice of values in the same order as the input keys.
// Keys not found in cache will have the zero value for T at their position.
// If any error other than ErrorNotFound occurs, the entire operation fails.
func MGet[T any](ctx context.Context, keys []string) ([]T, error) {
	if len(keys) == 0 {
		return []T{}, nil
	}

	if cacheType == TypeRedis && redisClient != nil {
		return mgetRedis[T](ctx, keys)
	}

	// Fallback for local cache: sequential gets
	results := make([]T, len(keys))
	for i, key := range keys {
		var val T
		if err := Get(ctx, key, &val); err == nil {
			results[i] = val
		} else if err == ErrorNotFound {
			// Keep zero value for T
		} else {
			return nil, err
		}
	}

	return results, nil
}

func mgetRedis[T any](ctx context.Context, keys []string) ([]T, error) {
	// Compose keys with prefix
	composedKeys := make([]string, len(keys))
	for i, key := range keys {
		composedKeys[i] = compose(key)
	}

	// Use Redis MGET
	vals, err := redisClient.MGet(ctx, composedKeys...).Result()
	if err != nil {
		return nil, err
	}

	// Process results in order
	results := make([]T, len(keys))
	for i, val := range vals {
		if val == nil {
			// Key not found, keep zero value for T
			continue
		}

		// val is the raw string stored in Redis
		strVal, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected value type for key %s: %T", keys[i], val)
		}

		if err := unmarshal([]byte(strVal), &results[i]); err != nil {
			return nil, fmt.Errorf("unmarshal failed for key %s: %w", keys[i], err)
		}
	}

	return results, nil
}
