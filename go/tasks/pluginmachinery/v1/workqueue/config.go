package workqueue

type Config struct {
	Workers    int `json:"workers"`
	MaxRetries int `json:"maxRetries"`
}
