# cache
Cache library for Apen services

## Config

| 欄位 | 說明 |
| --- | --- |
| `Prefix` | key 前綴，實際 key 為 `<Prefix>:<key>` |
| `Type` | `TypeLocal`（ristretto）或 `TypeRedis` |
| `RedisURL` | `TypeRedis` 必填 |
| `PoolSize` | Redis 連線池大小；0 沿用 go-redis 預設（每個 `GOMAXPROCS` 10 條） |

## API

| 函式 | 說明 |
| --- | --- |
| `Set` / `SetWithTTL` | 寫入單一 key |
| `MSetWithTTL` | 一次寫入多個 key、同一 TTL；Redis 用單一 pipeline 一次往返 |
| `Get` | 讀單一 key，不存在回 `ErrorNotFound` |
| `MGet` | 一次讀多個 key（Redis 用 `MGET`），不存在的位置為 `T` 的零值 |
| `Delete` | 刪除單一 key |

值以 gob 編碼。gob 會把空 slice 還原成 `nil`，`MGet` 無法區分「存了空 slice」與「不存在」；需要快取空結果時，存成帶非零欄位的 struct。
