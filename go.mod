module github.com/lyft/flyteplugins

go 1.13

require (
	cloud.google.com/go v0.55.0 // indirect
	github.com/Azure/azure-sdk-for-go v40.4.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest v0.10.0 // indirect
	github.com/GoogleCloudPlatform/spark-on-k8s-operator v0.1.3
	github.com/aws/aws-sdk-go v1.29.29
	github.com/coocood/freecache v1.1.0
	github.com/coreos/etcd v3.3.15+incompatible // indirect
	github.com/go-test/deep v1.0.5
	github.com/golang/protobuf v1.3.5
	github.com/googleapis/gnostic v0.4.1 // indirect
	github.com/graymeta/stow v0.2.5 // indirect
	github.com/hashicorp/golang-lru v0.5.4
	github.com/jmespath/go-jmespath v0.3.0 // indirect
	github.com/lyft/flyteidl v0.17.20
	github.com/lyft/flytestdlib v0.3.3
	github.com/magiconair/properties v1.8.1
	github.com/mattn/go-colorable v0.1.6 // indirect
	github.com/mitchellh/mapstructure v1.1.2
	github.com/ncw/swift v1.0.50 // indirect
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.5.1
	github.com/prometheus/procfs v0.0.11 // indirect
	github.com/spf13/cobra v0.0.6 // indirect
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.5.1
	golang.org/x/crypto v0.0.0-20200320181102-891825fb96df // indirect
	golang.org/x/net v0.0.0-20200320181208-1c781a10960a
	golang.org/x/sys v0.0.0-20200320181252-af34d8274f85 // indirect
	golang.org/x/tools v0.0.0-20200317043434-63da46f3035e
	google.golang.org/genproto v0.0.0-20200319113533-08878b785e9c // indirect
	google.golang.org/grpc v1.28.0
	gopkg.in/inf.v0 v0.9.1 // indirect
	k8s.io/api v0.17.4
	k8s.io/apimachinery v0.17.4
	k8s.io/client-go v11.0.0+incompatible
	k8s.io/utils v0.0.0-20200320200009-4a6ff033650d // indirect
	sigs.k8s.io/controller-runtime v0.5.1
	sigs.k8s.io/testing_frameworks v0.1.2 // indirect
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
