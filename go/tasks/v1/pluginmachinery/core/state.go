package core

type PluginStateWriter interface {
	Put(stateVersion uint8, v interface{}) error
	Reset() error
}

type PluginStateReader interface {
	GetStateVersion() uint8
	Get(t interface{}) (stateVersion uint8, err error)
}
