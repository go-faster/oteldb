module github.com/go-faster/oteldb

go 1.21.0

require (
	github.com/ClickHouse/ch-go v0.58.2
	github.com/Masterminds/sprig/v3 v3.2.3
	github.com/cenkalti/backoff/v4 v4.2.1
	github.com/cespare/xxhash/v2 v2.2.0
	github.com/docker/docker v24.0.6+incompatible
	github.com/dustin/go-humanize v1.0.1
	github.com/fatih/color v1.15.0
	github.com/go-faster/errors v0.6.1
	github.com/go-faster/jx v1.1.0
	github.com/go-faster/sdk v0.8.2
	github.com/go-faster/tcpproxy v0.1.0
	github.com/go-logfmt/logfmt v0.6.0
	github.com/google/uuid v1.3.1
	github.com/ogen-go/ogen v0.73.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/prometheus/common v0.44.0
	github.com/prometheus/prometheus v0.46.0
	github.com/pyroscope-io/client v0.7.2
	github.com/spf13/cobra v1.7.0
	github.com/stretchr/testify v1.8.4
	github.com/testcontainers/testcontainers-go v0.23.0
	go.opentelemetry.io/collector v0.84.0
	go.opentelemetry.io/collector/component v0.84.0
	go.opentelemetry.io/collector/confmap v0.84.0
	go.opentelemetry.io/collector/consumer v0.84.0
	go.opentelemetry.io/collector/exporter v0.84.0
	go.opentelemetry.io/collector/extension v0.84.0
	go.opentelemetry.io/collector/pdata v1.0.0-rcv0014
	go.opentelemetry.io/collector/receiver v0.84.0
	go.opentelemetry.io/collector/receiver/otlpreceiver v0.84.0
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.43.0
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.43.0
	go.opentelemetry.io/contrib/zpages v0.43.0
	go.opentelemetry.io/otel v1.17.0
	go.opentelemetry.io/otel/bridge/opentracing v1.17.0
	go.opentelemetry.io/otel/metric v1.17.0
	go.opentelemetry.io/otel/sdk v1.17.0
	go.opentelemetry.io/otel/trace v1.17.0
	go.opentelemetry.io/proto/otlp v1.0.0
	go.uber.org/multierr v1.11.0
	go.uber.org/zap v1.25.0
	go.ytsaurus.tech/library/go/core/log v0.0.3
	go.ytsaurus.tech/yt/go v0.0.9
	go4.org/netipx v0.0.0-20230303233057-f1b76eb4bb35
	golang.org/x/exp v0.0.0-20230725093048-515e97ebf090
	golang.org/x/sync v0.3.0
	golang.org/x/tools v0.13.0
	google.golang.org/grpc v1.58.0
)

require (
	contrib.go.opencensus.io/exporter/prometheus v0.4.2 // indirect
	dario.cat/mergo v1.0.0 // indirect
	github.com/Azure/go-ansiterm v0.0.0-20210617225240-d185dfc1b5a1 // indirect
	github.com/Masterminds/goutils v1.1.1 // indirect
	github.com/Masterminds/semver/v3 v3.2.0 // indirect
	github.com/Microsoft/go-winio v0.6.1 // indirect
	github.com/andybalholm/brotli v1.0.5 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/containerd/containerd v1.7.3 // indirect
	github.com/cpuguy83/dockercfg v0.3.1 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dlclark/regexp2 v1.10.0 // indirect
	github.com/dmarkham/enumer v1.5.8 // indirect
	github.com/docker/distribution v2.8.2+incompatible // indirect
	github.com/docker/go-connections v0.4.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/felixge/httpsnoop v1.0.3 // indirect
	github.com/fsnotify/fsnotify v1.6.0 // indirect
	github.com/ghodss/yaml v1.0.0 // indirect
	github.com/go-faster/city v1.0.1 // indirect
	github.com/go-faster/yaml v0.4.6 // indirect
	github.com/go-kit/log v0.2.1 // indirect
	github.com/go-logr/logr v1.2.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-logr/zapr v1.2.4 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/gofrs/uuid v4.2.0+incompatible // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/tink/go v1.7.0 // indirect
	github.com/grafana/regexp v0.0.0-20221122212121-6b5c0a4cb7fd // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.16.0 // indirect
	github.com/hashicorp/go-version v1.6.0 // indirect
	github.com/huandu/xstrings v1.3.3 // indirect
	github.com/imdario/mergo v0.3.16 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.16.7 // indirect
	github.com/knadh/koanf v1.5.0 // indirect
	github.com/knadh/koanf/v2 v2.0.1 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/magiconair/properties v1.8.7 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/mapstructure v1.5.1-0.20220423185008-bf980b35cac4 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/moby/patternmatcher v0.5.0 // indirect
	github.com/moby/sys/sequential v0.5.0 // indirect
	github.com/moby/term v0.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/morikuni/aec v1.0.0 // indirect
	github.com/mostynb/go-grpc-compression v1.2.0 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.1.0-rc4 // indirect
	github.com/opencontainers/runc v1.1.5 // indirect
	github.com/pascaldekloe/name v1.0.1 // indirect
	github.com/pierrec/lz4 v2.6.1+incompatible // indirect
	github.com/pierrec/lz4/v4 v4.1.18 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/prometheus/client_golang v1.16.0 // indirect
	github.com/prometheus/client_model v0.4.0 // indirect
	github.com/prometheus/procfs v0.11.0 // indirect
	github.com/prometheus/statsd_exporter v0.22.7 // indirect
	github.com/rs/cors v1.9.0 // indirect
	github.com/segmentio/asm v1.2.0 // indirect
	github.com/shirou/gopsutil/v3 v3.23.7 // indirect
	github.com/shoenig/go-m1cpu v0.1.6 // indirect
	github.com/shopspring/decimal v1.2.0 // indirect
	github.com/sirupsen/logrus v1.9.0 // indirect
	github.com/spf13/cast v1.3.1 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/tklauser/go-sysconf v0.3.11 // indirect
	github.com/tklauser/numcpus v0.6.0 // indirect
	github.com/yusufpapurcu/wmi v1.2.3 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/collector/config/configauth v0.84.0 // indirect
	go.opentelemetry.io/collector/config/configcompression v0.84.0 // indirect
	go.opentelemetry.io/collector/config/configgrpc v0.84.0 // indirect
	go.opentelemetry.io/collector/config/confighttp v0.84.0 // indirect
	go.opentelemetry.io/collector/config/confignet v0.84.0 // indirect
	go.opentelemetry.io/collector/config/configopaque v0.84.0 // indirect
	go.opentelemetry.io/collector/config/configtelemetry v0.84.0 // indirect
	go.opentelemetry.io/collector/config/configtls v0.84.0 // indirect
	go.opentelemetry.io/collector/config/internal v0.84.0 // indirect
	go.opentelemetry.io/collector/connector v0.84.0 // indirect
	go.opentelemetry.io/collector/extension/auth v0.84.0 // indirect
	go.opentelemetry.io/collector/featuregate v1.0.0-rcv0014 // indirect
	go.opentelemetry.io/collector/processor v0.84.0 // indirect
	go.opentelemetry.io/collector/semconv v0.84.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/runtime v0.42.0 // indirect
	go.opentelemetry.io/contrib/propagators/autoprop v0.42.0 // indirect
	go.opentelemetry.io/contrib/propagators/aws v1.17.0 // indirect
	go.opentelemetry.io/contrib/propagators/b3 v1.17.0 // indirect
	go.opentelemetry.io/contrib/propagators/jaeger v1.17.0 // indirect
	go.opentelemetry.io/contrib/propagators/ot v1.17.0 // indirect
	go.opentelemetry.io/otel/bridge/opencensus v0.39.0 // indirect
	go.opentelemetry.io/otel/exporters/jaeger v1.16.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/internal/retry v1.16.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric v0.39.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v0.39.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v0.39.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.16.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.16.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.16.0 // indirect
	go.opentelemetry.io/otel/exporters/prometheus v0.39.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v0.39.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.16.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v0.39.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.ytsaurus.tech/library/go/blockcodecs v0.0.2 // indirect
	go.ytsaurus.tech/library/go/core/xerrors v0.0.3 // indirect
	go.ytsaurus.tech/library/go/ptr v0.0.1 // indirect
	go.ytsaurus.tech/library/go/x/xreflect v0.0.2 // indirect
	go.ytsaurus.tech/library/go/x/xruntime v0.0.3 // indirect
	golang.org/x/crypto v0.13.0 // indirect
	golang.org/x/mod v0.12.0 // indirect
	golang.org/x/net v0.15.0 // indirect
	golang.org/x/sys v0.12.0 // indirect
	golang.org/x/text v0.13.0 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	gonum.org/v1/gonum v0.14.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20230717213848-3f92550aa753 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230717213848-3f92550aa753 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// https://github.com/aws-observability/aws-otel-collector/issues/926
//
// cloud.google.com/go/compute/metadata: ambiguous import: found package cloud.google.com/go/compute/metadata in multiple modules
//  cloud.google.com/go v0.65.0 (/go/pkg/mod/cloud.google.com/go@v0.65.0/compute/metadata)
//  cloud.google.com/go/compute/metadata v0.2.4-0.20230617002413-005d2dfb6b68 (/go/pkg/mod/cloud.google.com/go/compute/metadata@v0.2.4-0.20230617002413-005d2dfb6b68
replace cloud.google.com/go => cloud.google.com/go v0.100.2
