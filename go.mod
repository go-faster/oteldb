module github.com/go-faster/oteldb

go 1.21.0

require (
	entgo.io/ent v0.13.1
	github.com/ClickHouse/ch-go v0.61.5
	github.com/Masterminds/sprig/v3 v3.2.3
	github.com/VictoriaMetrics/easyproto v0.1.4
	github.com/cenkalti/backoff/v4 v4.3.0
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/cheggaaa/pb/v3 v3.1.5
	github.com/dmarkham/enumer v1.5.10
	github.com/dustin/go-humanize v1.0.1
	github.com/fatih/color v1.17.0
	github.com/go-faster/errors v0.7.1
	github.com/go-faster/jx v1.1.0
	github.com/go-faster/sdk v0.15.1
	github.com/go-faster/yaml v0.4.6
	github.com/go-logfmt/logfmt v0.6.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/snappy v0.0.4
	github.com/google/go-cmp v0.6.0
	github.com/google/go-github/v58 v58.0.0
	github.com/google/uuid v1.6.0
	github.com/grafana/loki/pkg/push v0.0.0-20240415130356-945a0f516ad2
	github.com/grafana/pyroscope-go v1.1.1
	github.com/jackc/pgx/v5 v5.6.0
	github.com/klauspost/compress v1.17.9
	github.com/kr/logfmt v0.0.0-20140226030751-b84e30acd515
	github.com/ogen-go/ogen v1.2.2
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil v0.104.0
	github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributesprocessor v0.104.0
	github.com/open-telemetry/opentelemetry-collector-contrib/processor/metricstransformprocessor v0.104.0
	github.com/open-telemetry/opentelemetry-collector-contrib/processor/resourceprocessor v0.104.0
	github.com/prometheus/client_golang v1.19.1
	github.com/prometheus/common v0.55.0
	github.com/prometheus/prometheus v0.53.0
	github.com/schollz/progressbar/v3 v3.14.4
	github.com/spf13/cobra v1.8.1
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.9.0
	github.com/testcontainers/testcontainers-go v0.31.0
	github.com/valyala/bytebufferpool v1.0.0
	github.com/zeebo/xxh3 v1.0.2
	go.opentelemetry.io/collector/component v0.104.0
	go.opentelemetry.io/collector/config/configauth v0.104.0
	go.opentelemetry.io/collector/config/confighttp v0.104.0
	go.opentelemetry.io/collector/config/configtls v0.104.0
	go.opentelemetry.io/collector/confmap v0.104.0
	go.opentelemetry.io/collector/confmap/converter/expandconverter v0.104.0
	go.opentelemetry.io/collector/consumer v0.104.0
	go.opentelemetry.io/collector/exporter v0.104.0
	go.opentelemetry.io/collector/featuregate v1.11.0
	go.opentelemetry.io/collector/otelcol v0.104.0
	go.opentelemetry.io/collector/pdata v1.11.0
	go.opentelemetry.io/collector/processor v0.104.0
	go.opentelemetry.io/collector/processor/batchprocessor v0.104.0
	go.opentelemetry.io/collector/receiver v0.104.0
	go.opentelemetry.io/collector/receiver/otlpreceiver v0.104.0
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.52.0
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.52.0
	go.opentelemetry.io/contrib/zpages v0.52.0
	go.opentelemetry.io/otel v1.28.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.28.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.28.0
	go.opentelemetry.io/otel/metric v1.28.0
	go.opentelemetry.io/otel/sdk v1.28.0
	go.opentelemetry.io/otel/sdk/metric v1.28.0
	go.opentelemetry.io/otel/trace v1.28.0
	go.uber.org/atomic v1.11.0
	go.uber.org/multierr v1.11.0
	go.uber.org/zap v1.27.0
	go4.org/netipx v0.0.0-20230303233057-f1b76eb4bb35
	golang.org/x/exp v0.0.0-20240506185415-9bf2ced13842
	golang.org/x/oauth2 v0.21.0
	golang.org/x/perf v0.0.0-20240604174448-3b48cf0e0164
	golang.org/x/sync v0.7.0
	golang.org/x/tools v0.22.0
	google.golang.org/grpc v1.64.0
	gopkg.in/yaml.v2 v2.4.0
	sigs.k8s.io/yaml v1.4.0
)

require (
	ariga.io/atlas v0.19.1-0.20240203083654-5948b60a8e43 // indirect
	dario.cat/mergo v1.0.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.6.0 // indirect
	github.com/Azure/go-ansiterm v0.0.0-20210617225240-d185dfc1b5a1 // indirect
	github.com/Masterminds/goutils v1.1.1 // indirect
	github.com/Masterminds/semver/v3 v3.2.0 // indirect
	github.com/Microsoft/go-winio v0.6.1 // indirect
	github.com/Microsoft/hcsshim v0.11.4 // indirect
	github.com/VividCortex/ewma v1.2.0 // indirect
	github.com/aclements/go-moremath v0.0.0-20210112150236-f10218a38794 // indirect
	github.com/agext/levenshtein v1.2.1 // indirect
	github.com/alecthomas/participle/v2 v2.1.1 // indirect
	github.com/apparentlymart/go-textseg/v13 v13.0.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/containerd/containerd v1.7.15 // indirect
	github.com/containerd/log v0.1.0 // indirect
	github.com/cpuguy83/dockercfg v0.3.1 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dennwc/varint v1.0.0 // indirect
	github.com/distribution/reference v0.5.0 // indirect
	github.com/dlclark/regexp2 v1.11.0 // indirect
	github.com/docker/docker v26.1.3+incompatible // indirect
	github.com/docker/go-connections v0.5.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/edsrzf/mmap-go v1.1.0 // indirect
	github.com/expr-lang/expr v1.16.9 // indirect
	github.com/facette/natsort v0.0.0-20181210072756-2cd4dd1e2dcb // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.7.0 // indirect
	github.com/ghodss/yaml v1.0.0 // indirect
	github.com/go-faster/city v1.0.1 // indirect
	github.com/go-kit/log v0.2.1 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-logr/zapr v1.3.0 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-openapi/inflect v0.19.0 // indirect
	github.com/go-viper/mapstructure/v2 v2.0.0-alpha.1 // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/google/go-querystring v1.1.0 // indirect
	github.com/grafana/pyroscope-go/godeltaprof v0.1.6 // indirect
	github.com/grafana/regexp v0.0.0-20240518133315-a468a5bfb3bc // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.20.0 // indirect
	github.com/hashicorp/go-version v1.7.0 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/hashicorp/hcl/v2 v2.13.0 // indirect
	github.com/huandu/xstrings v1.3.3 // indirect
	github.com/iancoleman/strcase v0.3.0 // indirect
	github.com/imdario/mergo v0.3.16 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/cpuid/v2 v2.0.9 // indirect
	github.com/knadh/koanf v1.5.0 // indirect
	github.com/knadh/koanf/v2 v2.1.1 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/magiconair/properties v1.8.7 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-runewidth v0.0.15 // indirect
	github.com/mitchellh/colorstring v0.0.0-20190213212951-d06e56a500db // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/go-wordwrap v1.0.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/moby/docker-image-spec v1.3.1 // indirect
	github.com/moby/patternmatcher v0.6.0 // indirect
	github.com/moby/sys/sequential v0.5.0 // indirect
	github.com/moby/sys/user v0.1.0 // indirect
	github.com/moby/term v0.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/morikuni/aec v1.0.0 // indirect
	github.com/mostynb/go-grpc-compression v1.2.3 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/olekukonko/tablewriter v0.0.5 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal v0.104.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/filter v0.104.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl v0.104.0 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.1.0 // indirect
	github.com/pascaldekloe/name v1.0.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/rs/cors v1.10.1 // indirect
	github.com/segmentio/asm v1.2.0 // indirect
	github.com/shirou/gopsutil/v3 v3.24.5 // indirect
	github.com/shirou/gopsutil/v4 v4.24.5 // indirect
	github.com/shoenig/go-m1cpu v0.1.6 // indirect
	github.com/shopspring/decimal v1.2.0 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/spf13/cast v1.3.1 // indirect
	github.com/tklauser/go-sysconf v0.3.12 // indirect
	github.com/tklauser/numcpus v0.6.1 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	github.com/zclconf/go-cty v1.8.0 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/collector v0.104.0 // indirect
	go.opentelemetry.io/collector/config/configcompression v1.11.0 // indirect
	go.opentelemetry.io/collector/config/configgrpc v0.104.0 // indirect
	go.opentelemetry.io/collector/config/confignet v0.104.0 // indirect
	go.opentelemetry.io/collector/config/configopaque v1.11.0 // indirect
	go.opentelemetry.io/collector/config/configretry v1.11.0 // indirect
	go.opentelemetry.io/collector/config/configtelemetry v0.104.0 // indirect
	go.opentelemetry.io/collector/config/internal v0.104.0 // indirect
	go.opentelemetry.io/collector/connector v0.104.0 // indirect
	go.opentelemetry.io/collector/extension v0.104.0 // indirect
	go.opentelemetry.io/collector/extension/auth v0.104.0 // indirect
	go.opentelemetry.io/collector/semconv v0.104.0 // indirect
	go.opentelemetry.io/collector/service v0.104.0 // indirect
	go.opentelemetry.io/contrib/config v0.7.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/runtime v0.51.0 // indirect
	go.opentelemetry.io/contrib/propagators/autoprop v0.51.0 // indirect
	go.opentelemetry.io/contrib/propagators/aws v1.26.0 // indirect
	go.opentelemetry.io/contrib/propagators/b3 v1.27.0 // indirect
	go.opentelemetry.io/contrib/propagators/jaeger v1.26.0 // indirect
	go.opentelemetry.io/contrib/propagators/ot v1.26.0 // indirect
	go.opentelemetry.io/otel/bridge/opencensus v1.27.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.27.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v1.27.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.27.0 // indirect
	go.opentelemetry.io/otel/exporters/prometheus v0.49.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v1.27.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.27.0 // indirect
	go.opentelemetry.io/proto/otlp v1.3.1 // indirect
	go.uber.org/automaxprocs v1.5.3 // indirect
	golang.org/x/crypto v0.24.0 // indirect
	golang.org/x/mod v0.18.0 // indirect
	golang.org/x/net v0.26.0 // indirect
	golang.org/x/sys v0.21.0 // indirect
	golang.org/x/term v0.21.0 // indirect
	golang.org/x/text v0.16.0 // indirect
	gonum.org/v1/gonum v0.15.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240701130421-f6361c86f094 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240701130421-f6361c86f094 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// https://github.com/aws-observability/aws-otel-collector/issues/926
//
// cloud.google.com/go/compute/metadata: ambiguous import: found package cloud.google.com/go/compute/metadata in multiple modules
//  cloud.google.com/go v0.65.0 (/go/pkg/mod/cloud.google.com/go@v0.65.0/compute/metadata)
//  cloud.google.com/go/compute/metadata v0.2.4-0.20230617002413-005d2dfb6b68 (/go/pkg/mod/cloud.google.com/go/compute/metadata@v0.2.4-0.20230617002413-005d2dfb6b68
replace cloud.google.com/go => cloud.google.com/go v0.100.2
