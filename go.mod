module github.com/go-faster/oteldb

go 1.23.0

require (
	entgo.io/ent v0.14.4
	github.com/ClickHouse/ch-go v0.67.0
	github.com/MakeNowJust/heredoc/v2 v2.0.1
	github.com/Masterminds/sprig/v3 v3.3.0
	github.com/VictoriaMetrics/easyproto v0.1.4
	github.com/cenkalti/backoff/v4 v4.3.0
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/cheggaaa/pb/v3 v3.1.7
	github.com/dmarkham/enumer v1.5.11
	github.com/docker/cli v28.3.2+incompatible
	github.com/docker/docker v28.3.2+incompatible
	github.com/dustin/go-humanize v1.0.1
	github.com/fatih/color v1.18.0
	github.com/go-faster/errors v0.7.1
	github.com/go-faster/jx v1.1.0
	github.com/go-faster/sdk v0.28.0
	github.com/go-faster/yaml v0.4.6
	github.com/go-logfmt/logfmt v0.6.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/snappy v1.0.0
	github.com/google/go-cmp v0.7.0
	github.com/google/uuid v1.6.0
	github.com/grafana/loki/pkg/push v0.0.0-20241223090937-bf7301470453
	github.com/grafana/pyroscope-go v1.2.4
	github.com/klauspost/compress v1.18.0
	github.com/kr/logfmt v0.0.0-20210122060352-19f9bcb100e6
	github.com/mattn/go-isatty v0.0.20
	github.com/ogen-go/ogen v1.14.0
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil v0.130.0
	github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributesprocessor v0.130.0
	github.com/open-telemetry/opentelemetry-collector-contrib/processor/metricstransformprocessor v0.130.0
	github.com/open-telemetry/opentelemetry-collector-contrib/processor/resourceprocessor v0.130.0
	github.com/prometheus/client_golang v1.22.0
	github.com/prometheus/common v0.65.0
	github.com/prometheus/prometheus v0.305.0
	github.com/schollz/progressbar/v3 v3.18.0
	github.com/spf13/cobra v1.9.1
	github.com/spf13/pflag v1.0.7
	github.com/stretchr/testify v1.10.0
	github.com/testcontainers/testcontainers-go v0.38.0
	github.com/valyala/bytebufferpool v1.0.0
	github.com/zeebo/xxh3 v1.0.2
	go.opentelemetry.io/collector/component v1.36.1
	go.opentelemetry.io/collector/component/componentstatus v0.130.1
	go.opentelemetry.io/collector/component/componenttest v0.130.1
	go.opentelemetry.io/collector/config/confighttp v0.130.1
	go.opentelemetry.io/collector/config/configoptional v0.130.1
	go.opentelemetry.io/collector/config/configtls v1.36.1
	go.opentelemetry.io/collector/confmap v1.36.1
	go.opentelemetry.io/collector/confmap/provider/envprovider v1.36.1
	go.opentelemetry.io/collector/consumer v1.36.1
	go.opentelemetry.io/collector/consumer/consumertest v0.130.1
	go.opentelemetry.io/collector/exporter v0.130.1
	go.opentelemetry.io/collector/otelcol v0.130.1
	go.opentelemetry.io/collector/pdata v1.36.1
	go.opentelemetry.io/collector/pipeline v0.130.1
	go.opentelemetry.io/collector/processor v1.36.1
	go.opentelemetry.io/collector/processor/batchprocessor v0.130.1
	go.opentelemetry.io/collector/receiver v1.36.1
	go.opentelemetry.io/collector/receiver/otlpreceiver v0.130.1
	go.opentelemetry.io/collector/receiver/receiverhelper v0.130.1
	go.opentelemetry.io/collector/receiver/receivertest v0.130.1
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.62.0
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.62.0
	go.opentelemetry.io/contrib/zpages v0.62.0
	go.opentelemetry.io/otel v1.37.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.37.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.37.0
	go.opentelemetry.io/otel/metric v1.37.0
	go.opentelemetry.io/otel/sdk v1.37.0
	go.opentelemetry.io/otel/sdk/metric v1.37.0
	go.opentelemetry.io/otel/trace v1.37.0
	go.uber.org/atomic v1.11.0
	go.uber.org/multierr v1.11.0
	go.uber.org/zap v1.27.0
	go4.org/netipx v0.0.0-20231129151722-fdeea329fbba
	golang.org/x/exp v0.0.0-20250106191152-7588d65b2ba8
	golang.org/x/perf v0.0.0-20241204221936-711ff2ab7231
	golang.org/x/sync v0.16.0
	golang.org/x/tools v0.35.0
	google.golang.org/grpc v1.74.2
	gopkg.in/yaml.v2 v2.4.0
	sigs.k8s.io/yaml v1.5.0
)

require (
	ariga.io/atlas v0.31.1-0.20250212144724-069be8033e83 // indirect
	dario.cat/mergo v1.0.1 // indirect
	github.com/Azure/go-ansiterm v0.0.0-20210617225240-d185dfc1b5a1 // indirect
	github.com/KimMachineGun/automemlimit v0.7.4 // indirect
	github.com/Masterminds/goutils v1.1.1 // indirect
	github.com/Masterminds/semver/v3 v3.3.0 // indirect
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/VividCortex/ewma v1.2.0 // indirect
	github.com/aclements/go-moremath v0.0.0-20210112150236-f10218a38794 // indirect
	github.com/agext/levenshtein v1.2.1 // indirect
	github.com/alecthomas/participle/v2 v2.1.4 // indirect
	github.com/antchfx/xmlquery v1.4.4 // indirect
	github.com/antchfx/xpath v1.3.4 // indirect
	github.com/apparentlymart/go-textseg/v13 v13.0.0 // indirect
	github.com/apparentlymart/go-textseg/v15 v15.0.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bmatcuk/doublestar v1.3.4 // indirect
	github.com/cenkalti/backoff/v5 v5.0.2 // indirect
	github.com/containerd/errdefs v1.0.0 // indirect
	github.com/containerd/errdefs/pkg v0.3.0 // indirect
	github.com/containerd/log v0.1.0 // indirect
	github.com/containerd/platforms v0.2.1 // indirect
	github.com/cpuguy83/dockercfg v0.3.2 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dennwc/varint v1.0.0 // indirect
	github.com/distribution/reference v0.6.0 // indirect
	github.com/dlclark/regexp2 v1.11.5 // indirect
	github.com/docker/distribution v2.8.3+incompatible // indirect
	github.com/docker/docker-credential-helpers v0.7.0 // indirect
	github.com/docker/go-connections v0.5.0 // indirect
	github.com/docker/go-metrics v0.0.1 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/ebitengine/purego v0.8.4 // indirect
	github.com/edsrzf/mmap-go v1.2.0 // indirect
	github.com/elastic/go-grok v0.3.1 // indirect
	github.com/elastic/lunes v0.1.0 // indirect
	github.com/expr-lang/expr v1.17.5 // indirect
	github.com/facette/natsort v0.0.0-20181210072756-2cd4dd1e2dcb // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/foxboron/go-tpm-keyfiles v0.0.0-20250323135004-b31fac66206e // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/fvbommel/sortorder v1.1.0 // indirect
	github.com/ghodss/yaml v1.0.0 // indirect
	github.com/go-faster/city v1.0.1 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-logr/zapr v1.3.0 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-openapi/inflect v0.19.0 // indirect
	github.com/go-viper/mapstructure/v2 v2.3.0 // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/google/go-tpm v0.9.5 // indirect
	github.com/gorilla/mux v1.8.1 // indirect
	github.com/grafana/otel-profiling-go v0.5.1 // indirect
	github.com/grafana/pyroscope-go/godeltaprof v0.1.8 // indirect
	github.com/grafana/regexp v0.0.0-20240518133315-a468a5bfb3bc // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.27.1 // indirect
	github.com/hashicorp/go-version v1.7.0 // indirect
	github.com/hashicorp/golang-lru v0.6.0 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/hashicorp/hcl/v2 v2.13.0 // indirect
	github.com/huandu/xstrings v1.5.0 // indirect
	github.com/iancoleman/strcase v0.3.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/cpuid/v2 v2.0.9 // indirect
	github.com/knadh/koanf v1.5.0 // indirect
	github.com/knadh/koanf/v2 v2.2.1 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/magefile/mage v1.15.0 // indirect
	github.com/magiconair/properties v1.8.10 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/miekg/pkcs11 v1.1.1 // indirect
	github.com/mitchellh/colorstring v0.0.0-20190213212951-d06e56a500db // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/go-wordwrap v1.0.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/moby/docker-image-spec v1.3.1 // indirect
	github.com/moby/go-archive v0.1.0 // indirect
	github.com/moby/patternmatcher v0.6.0 // indirect
	github.com/moby/sys/atomicwriter v0.1.0 // indirect
	github.com/moby/sys/sequential v0.6.0 // indirect
	github.com/moby/sys/user v0.4.0 // indirect
	github.com/moby/sys/userns v0.1.0 // indirect
	github.com/moby/term v0.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/morikuni/aec v1.0.0 // indirect
	github.com/mostynb/go-grpc-compression v1.2.3 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/olekukonko/tablewriter v0.0.5 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal v0.130.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/filter v0.130.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl v0.130.0 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.1.1 // indirect
	github.com/pascaldekloe/name v1.0.1 // indirect
	github.com/pbnjay/memory v0.0.0-20210728143218-7b4eea64cf58 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/otlptranslator v0.0.0-20250717125610-8549f4ab4f8f // indirect
	github.com/prometheus/procfs v0.17.0 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/rs/cors v1.11.1 // indirect
	github.com/samber/lo v1.47.0 // indirect
	github.com/samber/slog-common v0.18.1 // indirect
	github.com/samber/slog-zap/v2 v2.6.2 // indirect
	github.com/segmentio/asm v1.2.0 // indirect
	github.com/shirou/gopsutil/v4 v4.25.6 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/spf13/cast v1.7.0 // indirect
	github.com/theupdateframework/notary v0.7.0 // indirect
	github.com/tklauser/go-sysconf v0.3.12 // indirect
	github.com/tklauser/numcpus v0.6.1 // indirect
	github.com/twmb/murmur3 v1.1.8 // indirect
	github.com/ua-parser/uap-go v0.0.0-20240611065828-3a4781585db6 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	github.com/zclconf/go-cty v1.14.4 // indirect
	github.com/zclconf/go-cty-yaml v1.1.0 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/collector v0.130.1 // indirect
	go.opentelemetry.io/collector/client v1.36.1 // indirect
	go.opentelemetry.io/collector/config/configauth v0.130.1 // indirect
	go.opentelemetry.io/collector/config/configcompression v1.36.1 // indirect
	go.opentelemetry.io/collector/config/configgrpc v0.130.1 // indirect
	go.opentelemetry.io/collector/config/configmiddleware v0.130.1 // indirect
	go.opentelemetry.io/collector/config/confignet v1.36.1 // indirect
	go.opentelemetry.io/collector/config/configopaque v1.36.1 // indirect
	go.opentelemetry.io/collector/config/configretry v1.36.1 // indirect
	go.opentelemetry.io/collector/config/configtelemetry v0.130.1 // indirect
	go.opentelemetry.io/collector/confmap/xconfmap v0.130.1 // indirect
	go.opentelemetry.io/collector/connector v0.130.1 // indirect
	go.opentelemetry.io/collector/connector/connectortest v0.130.1 // indirect
	go.opentelemetry.io/collector/connector/xconnector v0.130.1 // indirect
	go.opentelemetry.io/collector/consumer/consumererror v0.130.1 // indirect
	go.opentelemetry.io/collector/consumer/xconsumer v0.130.1 // indirect
	go.opentelemetry.io/collector/exporter/exportertest v0.130.1 // indirect
	go.opentelemetry.io/collector/exporter/xexporter v0.130.1 // indirect
	go.opentelemetry.io/collector/extension v1.36.1 // indirect
	go.opentelemetry.io/collector/extension/extensionauth v1.36.1 // indirect
	go.opentelemetry.io/collector/extension/extensioncapabilities v0.130.1 // indirect
	go.opentelemetry.io/collector/extension/extensionmiddleware v0.130.1 // indirect
	go.opentelemetry.io/collector/extension/extensiontest v0.130.1 // indirect
	go.opentelemetry.io/collector/extension/xextension v0.130.1 // indirect
	go.opentelemetry.io/collector/featuregate v1.36.1 // indirect
	go.opentelemetry.io/collector/internal/fanoutconsumer v0.130.1 // indirect
	go.opentelemetry.io/collector/internal/sharedcomponent v0.130.1 // indirect
	go.opentelemetry.io/collector/internal/telemetry v0.130.1 // indirect
	go.opentelemetry.io/collector/pdata/pprofile v0.130.1 // indirect
	go.opentelemetry.io/collector/pdata/testdata v0.130.1 // indirect
	go.opentelemetry.io/collector/pdata/xpdata v0.130.1 // indirect
	go.opentelemetry.io/collector/pipeline/xpipeline v0.130.1 // indirect
	go.opentelemetry.io/collector/processor/processorhelper v0.130.0 // indirect
	go.opentelemetry.io/collector/processor/processorhelper/xprocessorhelper v0.130.0 // indirect
	go.opentelemetry.io/collector/processor/processortest v0.130.1 // indirect
	go.opentelemetry.io/collector/processor/xprocessor v0.130.1 // indirect
	go.opentelemetry.io/collector/receiver/xreceiver v0.130.1 // indirect
	go.opentelemetry.io/collector/service v0.130.1 // indirect
	go.opentelemetry.io/collector/service/hostcapabilities v0.130.1 // indirect
	go.opentelemetry.io/contrib/bridges/otelzap v0.12.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/runtime v0.62.0 // indirect
	go.opentelemetry.io/contrib/otelconf v0.17.0 // indirect
	go.opentelemetry.io/contrib/propagators/autoprop v0.62.0 // indirect
	go.opentelemetry.io/contrib/propagators/aws v1.37.0 // indirect
	go.opentelemetry.io/contrib/propagators/b3 v1.37.0 // indirect
	go.opentelemetry.io/contrib/propagators/jaeger v1.37.0 // indirect
	go.opentelemetry.io/contrib/propagators/ot v1.37.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc v0.13.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp v0.13.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.37.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v1.37.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.37.0 // indirect
	go.opentelemetry.io/otel/exporters/prometheus v0.59.1 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutlog v0.13.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v1.37.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.37.0 // indirect
	go.opentelemetry.io/otel/log v0.13.0 // indirect
	go.opentelemetry.io/otel/sdk/log v0.13.0 // indirect
	go.opentelemetry.io/proto/otlp v1.7.0 // indirect
	go.uber.org/automaxprocs v1.6.0 // indirect
	go.yaml.in/yaml/v2 v2.4.2 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/crypto v0.40.0 // indirect
	golang.org/x/mod v0.26.0 // indirect
	golang.org/x/net v0.42.0 // indirect
	golang.org/x/sys v0.34.0 // indirect
	golang.org/x/term v0.33.0 // indirect
	golang.org/x/text v0.27.0 // indirect
	golang.org/x/tools/go/packages/packagestest v0.1.1-deprecated // indirect
	gonum.org/v1/gonum v0.16.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250603155806-513f23925822 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250603155806-513f23925822 // indirect
	google.golang.org/protobuf v1.36.6 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
