// Code generated by go generate; DO NOT EDIT.
// This file was generated by robots.

package k8s

import (
	"encoding/json"
	"reflect"

	"fmt"

	"github.com/spf13/pflag"
)

// If v is a pointer, it will get its element value or the zero value of the element type.
// If v is not a pointer, it will return it as is.
func (Config) elemValueOrNil(v interface{}) interface{} {
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

func (Config) mustMarshalJSON(v json.Marshaler) string {
	raw, err := v.MarshalJSON()
	if err != nil {
		panic(err)
	}

	return string(raw)
}

// GetPFlagSet will return strongly types pflags for all fields in Config and its nested types. The format of the
// flags is json-name.json-sub-name... etc.
func (cfg Config) GetPFlagSet(prefix string) *pflag.FlagSet {
	cmdFlags := pflag.NewFlagSet("Config", pflag.ExitOnError)
	cmdFlags.String(fmt.Sprintf("%v%v", prefix, "scheduler"), defaultConfig.DefaultScheduler, "Decides the scheduler to use when launching array-pods.")
	cmdFlags.Int(fmt.Sprintf("%v%v", prefix, "maxErrorLength"), defaultConfig.MaxErrorStringLength, "Determines the maximum length of the error string returned for the array.")
	cmdFlags.Int64(fmt.Sprintf("%v%v", prefix, "maxArrayJobSize"), defaultConfig.MaxArrayJobSize, "Maximum size of array job.")
	cmdFlags.Int(fmt.Sprintf("%v%v", prefix, "OutputAssembler.workers"), defaultConfig.OutputAssembler.Workers, "Number of concurrent workers to start processing the queue.")
	cmdFlags.Int(fmt.Sprintf("%v%v", prefix, "OutputAssembler.maxRetries"), defaultConfig.OutputAssembler.MaxRetries, "Maximum number of retries per item.")
	cmdFlags.Int(fmt.Sprintf("%v%v", prefix, "OutputAssembler.maxItems"), defaultConfig.OutputAssembler.IndexCacheMaxItems, "Maximum number of entries to keep in the index.")
	cmdFlags.Int(fmt.Sprintf("%v%v", prefix, "ErrorAssembler.workers"), defaultConfig.ErrorAssembler.Workers, "Number of concurrent workers to start processing the queue.")
	cmdFlags.Int(fmt.Sprintf("%v%v", prefix, "ErrorAssembler.maxRetries"), defaultConfig.ErrorAssembler.MaxRetries, "Maximum number of retries per item.")
	cmdFlags.Int(fmt.Sprintf("%v%v", prefix, "ErrorAssembler.maxItems"), defaultConfig.ErrorAssembler.IndexCacheMaxItems, "Maximum number of entries to keep in the index.")
	cmdFlags.Bool(fmt.Sprintf("%v%v", prefix, "logs.logs.cloudwatch-enabled"), defaultConfig.LogConfig.Logs.IsCloudwatchEnabled, "Enable Cloudwatch Logging")
	cmdFlags.String(fmt.Sprintf("%v%v", prefix, "logs.logs.cloudwatch-region"), defaultConfig.LogConfig.Logs.CloudwatchRegion, "AWS region in which Cloudwatch logs are stored.")
	cmdFlags.String(fmt.Sprintf("%v%v", prefix, "logs.logs.cloudwatch-log-group"), defaultConfig.LogConfig.Logs.CloudwatchLogGroup, "Log group to which streams are associated.")
	cmdFlags.String(fmt.Sprintf("%v%v", prefix, "logs.logs.cloudwatch-template-uri"), defaultConfig.LogConfig.Logs.CloudwatchTemplateURI, "Template Uri to use when building cloudwatch log links")
	cmdFlags.Bool(fmt.Sprintf("%v%v", prefix, "logs.logs.kubernetes-enabled"), defaultConfig.LogConfig.Logs.IsKubernetesEnabled, "Enable Kubernetes Logging")
	cmdFlags.String(fmt.Sprintf("%v%v", prefix, "logs.logs.kubernetes-url"), defaultConfig.LogConfig.Logs.KubernetesURL, "Console URL for Kubernetes logs")
	cmdFlags.String(fmt.Sprintf("%v%v", prefix, "logs.logs.kubernetes-template-uri"), defaultConfig.LogConfig.Logs.KubernetesTemplateURI, "Template Uri to use when building kubernetes log links")
	cmdFlags.Bool(fmt.Sprintf("%v%v", prefix, "logs.logs.stackdriver-enabled"), defaultConfig.LogConfig.Logs.IsStackDriverEnabled, "Enable Log-links to stackdriver")
	cmdFlags.String(fmt.Sprintf("%v%v", prefix, "logs.logs.gcp-project"), defaultConfig.LogConfig.Logs.GCPProjectName, "Name of the project in GCP")
	cmdFlags.String(fmt.Sprintf("%v%v", prefix, "logs.logs.stackdriver-logresourcename"), defaultConfig.LogConfig.Logs.StackdriverLogResourceName, "Name of the logresource in stackdriver")
	cmdFlags.String(fmt.Sprintf("%v%v", prefix, "logs.logs.stackdriver-template-uri"), defaultConfig.LogConfig.Logs.StackDriverTemplateURI, "Template Uri to use when building stackdriver log links")
	return cmdFlags
}
