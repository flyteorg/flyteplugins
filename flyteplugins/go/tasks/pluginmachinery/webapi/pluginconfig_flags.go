// Code generated by go generate; DO NOT EDIT.
// This file was generated by robots.

package webapi

import (
	"encoding/json"
	"reflect"

	"fmt"

	"github.com/spf13/pflag"
)

// If v is a pointer, it will get its element value or the zero value of the element type.
// If v is not a pointer, it will return it as is.
func (PluginConfig) elemValueOrNil(v interface{}) interface{} {
	if t := reflect.TypeOf(v); t.Kind() == reflect.Ptr {
		if reflect.ValueOf(v).IsNil() {
			return reflect.Zero(t.Elem()).Interface()
		} else {
			return reflect.ValueOf(v).Interface()
		}
	} else if v == nil {
		return reflect.Zero(t).Interface()
	}

	return v
}

func (PluginConfig) mustMarshalJSON(v json.Marshaler) string {
	raw, err := v.MarshalJSON()
	if err != nil {
		panic(err)
	}

	return string(raw)
}

// GetPFlagSet will return strongly types pflags for all fields in PluginConfig and its nested types. The format of the
// flags is json-name.json-sub-name... etc.
func (cfg PluginConfig) GetPFlagSet(prefix string) *pflag.FlagSet {
	cmdFlags := pflag.NewFlagSet("PluginConfig", pflag.ExitOnError)
	cmdFlags.Int(fmt.Sprintf("%v%v", prefix, "readRateLimiter.qps"), DefaultPluginConfig.ReadRateLimiter.QPS, "Defines the max rate of calls per second.")
	cmdFlags.Int(fmt.Sprintf("%v%v", prefix, "readRateLimiter.burst"), DefaultPluginConfig.ReadRateLimiter.Burst, "Defines the maximum burst size.")
	cmdFlags.Int(fmt.Sprintf("%v%v", prefix, "writeRateLimiter.qps"), DefaultPluginConfig.WriteRateLimiter.QPS, "Defines the max rate of calls per second.")
	cmdFlags.Int(fmt.Sprintf("%v%v", prefix, "writeRateLimiter.burst"), DefaultPluginConfig.WriteRateLimiter.Burst, "Defines the maximum burst size.")
	cmdFlags.Int(fmt.Sprintf("%v%v", prefix, "caching.size"), DefaultPluginConfig.Caching.Size, "Defines the maximum number of items to cache.")
	cmdFlags.String(fmt.Sprintf("%v%v", prefix, "caching.resyncInterval"), DefaultPluginConfig.Caching.ResyncInterval.String(), "Defines the sync interval.")
	cmdFlags.Int(fmt.Sprintf("%v%v", prefix, "caching.workers"), DefaultPluginConfig.Caching.Workers, "Defines the number of workers to start up to process items.")
	cmdFlags.Int(fmt.Sprintf("%v%v", prefix, "caching.maxSystemFailures"), DefaultPluginConfig.Caching.MaxSystemFailures, "Defines the number of failures to fetch a task before failing the task.")
	return cmdFlags
}