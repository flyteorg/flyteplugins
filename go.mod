module github.com/flyteorg/flyteplugins

go 1.13

require (
	github.com/GoogleCloudPlatform/spark-on-k8s-operator v0.0.0-20200210170106-c8ae9e352035
	github.com/Masterminds/semver v1.5.0
	github.com/aws/amazon-sagemaker-operator-for-k8s v1.1.0
	github.com/aws/aws-sdk-go v1.37.3
	github.com/aws/aws-sdk-go-v2 v1.2.0
	github.com/aws/aws-sdk-go-v2/config v1.1.1
	github.com/aws/aws-sdk-go-v2/service/athena v1.1.1
	github.com/coocood/freecache v1.1.1
	github.com/flyteorg/flyteidl v0.18.15
	github.com/flyteorg/flytestdlib v0.3.13
	github.com/go-test/deep v1.0.7
	github.com/golang/protobuf v1.4.3
	github.com/hashicorp/golang-lru v0.5.4
	github.com/kubeflow/pytorch-operator v0.6.0
	github.com/kubeflow/tf-operator v0.5.3
	github.com/magiconair/properties v1.8.4
	github.com/mitchellh/mapstructure v1.4.1
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.9.0
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.0
	golang.org/x/net v0.0.0-20210119194325-5f4716e94777
	google.golang.org/grpc v1.35.0
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f // indirect
	gotest.tools v2.2.0+incompatible
	k8s.io/api v0.20.4
	k8s.io/apimachinery v0.20.4
	k8s.io/client-go v11.0.1-0.20190918222721-c0e3722d5cf0+incompatible
	k8s.io/utils v0.0.0-20210111153108-fddb29f9d009
	sigs.k8s.io/controller-runtime v0.8.2
)

replace (
	github.com/GoogleCloudPlatform/spark-on-k8s-operator => github.com/lyft/spark-on-k8s-operator v0.1.4-0.20201027003055-c76b67e3b6d0
	github.com/aws/amazon-sagemaker-operator-for-k8s => github.com/aws/amazon-sagemaker-operator-for-k8s v1.0.1-0.20210303003444-0fb33b1fd49d

	github.com/flyteorg/flyteidl => github.com/flyteorg/flyteidl v0.18.15
	github.com/flyteorg/flytestdlib => github.com/flyteorg/flytestdlib v0.3.13
	google.golang.org/genproto => google.golang.org/genproto v0.0.0-20200205142000-a86caf926a67
	google.golang.org/grpc => google.golang.org/grpc v1.28.0
	k8s.io/client-go => k8s.io/client-go v0.20.2

)
