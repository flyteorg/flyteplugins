module github.com/lyft/flyteplugins

go 1.13

require (
	github.com/GoogleCloudPlatform/spark-on-k8s-operator v0.0.0-20200210170106-c8ae9e352035
	github.com/Jeffail/gabs/v2 v2.0.0 // indirect
	github.com/Masterminds/semver v1.5.0
	github.com/adammck/venv v0.0.0-20160819025605-8a9c907a37d3 // indirect
	github.com/antihax/optional v1.0.0 // indirect
	github.com/aws/amazon-sagemaker-operator-for-k8s v1.1.0
	github.com/aws/aws-sdk-go v1.37.1
	github.com/aws/aws-sdk-go-v2 v1.2.0
	github.com/aws/aws-sdk-go-v2/config v1.1.1
	github.com/aws/aws-sdk-go-v2/service/athena v1.1.1
	github.com/coocood/freecache v1.1.1
	github.com/coreos/go-oidc v2.1.0+incompatible // indirect
	github.com/docker/docker v0.7.3-0.20190327010347-be7ac8be2ae0 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/go-openapi/validate v0.19.5 // indirect
	github.com/go-test/deep v1.0.7
	github.com/golang/protobuf v1.4.3
	github.com/gophercloud/gophercloud v0.1.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.12.2 // indirect
	github.com/hashicorp/golang-lru v0.5.4
	github.com/influxdata/influxdb v1.7.9 // indirect
	github.com/kubeflow/pytorch-operator v0.6.0
	github.com/kubeflow/tf-operator v0.5.3
	github.com/lyft/flyteidl v0.18.14
	github.com/lyft/flytestdlib v0.3.12
	github.com/magiconair/properties v1.8.4
	github.com/mitchellh/mapstructure v1.4.1
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/natefinch/lumberjack v2.0.0+incompatible // indirect
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/pkg/errors v0.9.1
	github.com/pquerna/cachecontrol v0.0.0-20180517163645-1555304b9b35 // indirect
	github.com/prometheus/client_golang v1.9.0
	github.com/satori/uuid v1.2.0 // indirect
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.0
	golang.org/x/net v0.0.0-20210119194325-5f4716e94777
	gonum.org/v1/netlib v0.0.0-20190331212654-76723241ea4e // indirect
	google.golang.org/grpc v1.35.0
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 // indirect
	gopkg.in/square/go-jose.v2 v2.4.1 // indirect
	gopkg.in/yaml.v1 v1.0.0-20140924161607-9f9df34309c0 // indirect
	gotest.tools v2.2.0+incompatible
	k8s.io/api v0.20.2
	k8s.io/apimachinery v0.20.2
	k8s.io/client-go v0.20.4
	k8s.io/utils v0.0.0-20210111153108-fddb29f9d009
	sigs.k8s.io/controller-runtime v0.8.2
	sigs.k8s.io/structured-merge-diff v0.0.0-20190302045857-e85c7b244fd2 // indirect
)

replace (
	github.com/GoogleCloudPlatform/spark-on-k8s-operator => github.com/lyft/spark-on-k8s-operator v0.1.4-0.20201027003055-c76b67e3b6d0
	github.com/lyft/flytestdlib => github.com/flyteorg/flytestdlib v0.3.13
	github.com/lyft/flyteidl => github.com/flyteorg/flyteidl v0.18.15
)
