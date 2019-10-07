package workqueue

// Config for the queue
type Config struct {
	Workers            int `json:"workers"`
	MaxRetries         int `json:"maxRetries"`
	IndexCacheMaxItems int `json:"maxItems"`
}
