module github.com/lyft/flyteplugins

go 1.13

require (
	github.com/Azure/azure-sdk-for-go v39.0.0+incompatible // indirect
	github.com/GoogleCloudPlatform/spark-on-k8s-operator v0.1.3
	github.com/Masterminds/semver v1.5.0
	github.com/aws/amazon-sagemaker-operator-for-k8s v1.0.1-0.20200410212604-780c48ecb21a
	github.com/aws/aws-sdk-go v1.28.11
	github.com/coocood/freecache v1.1.0
	github.com/fsnotify/fsnotify v1.4.8-0.20191012010759-4bf2d1fec783
	github.com/go-test/deep v1.0.5
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.3.3
	github.com/googleapis/gnostic v0.4.1 // indirect
	github.com/hashicorp/golang-lru v0.5.4
	github.com/kubeflow/pytorch-operator v0.6.0
	github.com/kubeflow/tf-operator v0.5.3
	github.com/lyft/flyteidl v0.17.33-0.20200624052212-70a213e49b56
	github.com/lyft/flytestdlib v0.3.9
	github.com/magiconair/properties v1.8.1
	github.com/mitchellh/go-ps v1.0.0
	github.com/mitchellh/mapstructure v1.1.2
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.4.0
	github.com/spf13/cobra v0.0.5
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.6.1
	go.opencensus.io v0.22.3 // indirect
	golang.org/x/net v0.0.0-20200324143707-d3edc9973b7e
	golang.org/x/tools v0.0.0-20200124170513-3f4d10fc73b4
	google.golang.org/api v0.16.0 // indirect
	google.golang.org/genproto v0.0.0-20200205142000-a86caf926a67 // indirect
	google.golang.org/grpc v1.27.1
	gopkg.in/yaml.v2 v2.2.8
	gopkg.in/yaml.v3 v3.0.0-20200605160147-a5ece683394c // indirect
	gotest.tools v2.2.0+incompatible
	k8s.io/api v0.17.2
	k8s.io/apimachinery v0.17.2
	k8s.io/client-go v11.0.1-0.20190918222721-c0e3722d5cf0+incompatible
	k8s.io/klog v1.0.0
	k8s.io/utils v0.0.0-20200124190032-861946025e34 // indirect
	sigs.k8s.io/controller-runtime v0.4.0
	sigs.k8s.io/yaml v1.2.0 // indirect
)

// Pin the version of client-go to something that's compatible with katrogan's fork of api and apimachinery
// Type the following
//   replace k8s.io/client-go => k8s.io/client-go kubernetes-1.16.2
// and it will be replaced with the 'sha' variant of the version

replace (
	github.com/GoogleCloudPlatform/spark-on-k8s-operator => github.com/lyft/spark-on-k8s-operator v0.1.3
	github.com/googleapis/gnostic => github.com/googleapis/gnostic v0.3.1
	k8s.io/api => github.com/lyft/api v0.0.0-20191031200350-b49a72c274e0
	k8s.io/apimachinery => github.com/lyft/apimachinery v0.0.0-20191031200210-047e3ea32d7f
	k8s.io/client-go => k8s.io/client-go v0.0.0-20191016111102-bec269661e48
)
