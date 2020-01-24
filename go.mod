module github.com/lyft/flyteplugins

go 1.13

require (
	cloud.google.com/go v0.52.0 // indirect
	github.com/Azure/azure-sdk-for-go v38.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest v0.9.4 // indirect
	github.com/Azure/go-autorest/autorest/adal v0.8.1 // indirect
	github.com/Azure/go-autorest/autorest/to v0.3.0 // indirect
	github.com/GoogleCloudPlatform/spark-on-k8s-operator v0.1.3
	github.com/aws/aws-sdk-go v1.28.8
	github.com/coocood/freecache v1.1.0
	github.com/fatih/color v1.9.0 // indirect
	github.com/go-test/deep v1.0.5
	github.com/gogo/protobuf v1.3.1 // indirect
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e // indirect
	github.com/golang/protobuf v1.3.2
	github.com/google/gofuzz v1.1.0 // indirect
	github.com/graymeta/stow v0.2.4 // indirect
	github.com/hashicorp/golang-lru v0.5.4
	github.com/json-iterator/go v1.1.9 // indirect
	github.com/lyft/flyteidl v0.16.6
	github.com/lyft/flytestdlib v0.2.31
	github.com/magiconair/properties v1.8.1
	github.com/mattn/go-isatty v0.0.12 // indirect
	github.com/mitchellh/mapstructure v1.1.2
	github.com/pelletier/go-toml v1.6.0 // indirect
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.3.0
	github.com/prometheus/common v0.9.1 // indirect
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.4.0
	golang.org/x/crypto v0.0.0-20200117160349-530e935923ad // indirect
	golang.org/x/net v0.0.0-20200114155413-6afb5195e5aa
	golang.org/x/sys v0.0.0-20200122134326-e047566fdf82 // indirect
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0 // indirect
	google.golang.org/genproto v0.0.0-20200122232147-0452cf42e150 // indirect
	google.golang.org/grpc v1.26.0
	gopkg.in/yaml.v2 v2.2.8 // indirect
	k8s.io/api v0.17.2
	k8s.io/apimachinery v0.17.2
	k8s.io/client-go v11.0.0+incompatible
	k8s.io/utils v0.0.0-20200122174043-1e243dd1a584 // indirect
	sigs.k8s.io/controller-runtime v0.4.0
)

replace (
	github.com/GoogleCloudPlatform/spark-on-k8s-operator => github.com/lyft/spark-on-k8s-operator v0.1.3
	k8s.io/api => github.com/lyft/api v0.0.0-20191031200350-b49a72c274e0
	k8s.io/apimachinery => github.com/lyft/apimachinery v0.0.0-20191031200210-047e3ea32d7f
	k8s.io/client-go => k8s.io/client-go v0.0.0-20191016111102-bec269661e48
)

// k8s.io/client-go kubernetes-1.17.2
