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
		rdb := redis.NewClient(&redis.Options{
			Addr: c.RedisURL,
		})
		cacheStore = redis_store.NewRedis(rdb)
	default:
		panic(fmt.Errorf("unknown cache type"))

	}
	cacheManager = cache.New[string](cacheStore)
	keyPrefix = c.Prefix
}

func Finalize() {}

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
