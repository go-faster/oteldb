# Logs model

## OpenTelemetry

The OpenTelemetry log model can be marshalled as JSON document:

```json
{
  "body": "hello world",
  "traceid": "cacb16ac5ace52ebe8749114234a8e58",
  "spanid": "36a431d0481b2744",
  "severity": "Info",
  "flags": 1,
  "attributes": {
    "http.duration": 1105000000,
    "http.duration_seconds": 1.1054,
    "http.method": "GET",
    "http.server": true,
    "http.status_code": 200,
    "http.user_agent": "test-agent",
    "sdkVersion": "1.0.1"
  },
  "resources": {
    "host.name": "testHost"
  },
  "instrumentation_scope": {
    "name": "name",
    "version": "version",
    "attributes": {
      "oteldb.name": "testDB"
    }
  }
}
```

## LogQL

The LogQL unlike TraceQL inherits Prometheus label model constraints, i.e. no dots in label names
and label value is only string.

Using example above, lets explore our options for mapping.

### Resource attributes

Resource attributes can be straightforwardly mapped to labels:

```
otel_logs{host_name="testHost"}
```

Dots are replaced with underscores. 

> [!WARNING]  
> Mapping is not reversible, so should be persisted.

Optionally, we can prefix them with `resource_`.

### Attributes

It is unclear whether attributes should be mapped to labels.
On one hand, it is possible to do so:

```
{http_method="GET"}
```

Optionally with `attribute_` prefix.

On the other hand, it is desirable to make more complex queries, e.g.:

```
{service_name="testService"} | json | attributes_http_duration > 30s or attributes_status_code != 200 
```

This approach raises multiple questions:
1. Should we really use `attributes_` prefix?
2. If not, should we search both in attributes, resource and scope?
3. How to deal with grafana auto-detection of labels? Should we change json representation?

### Line filter expression

```
{job="testNamespace/testService"} |= `hello world`
```

1. Should we search only in body?
2. Should we search in all fields?
3. How to deal with traceID and spanID when using trace-to-logs?


## Draft solution

Log message:

```json
{
  "body": "hello world",
  "trace_id": "cacb16ac5ace52ebe8749114234a8e58",
  "span_id": "36a431d0481b2744",
  "severity": "INFO",
  "http.duration": 1105000000,
  "http.duration.seconds": 1.1054,
  "http.method": "GET",
  "http.server": true,
  "http.status_code": 200,
  "http.user_agent": "test-agent",
}
```

Labels:

```
severity="INFO" (should be from severity level as per standard)
service_name="testService"
service_namespace="testNamespace"

// Not sure about this one, will require storing mapping to support regex
job="testNamespace/testService".

// Optionally:
http_method="GET"
http_server="true"
http_status_code="200"
```

This will allow filtering by this labels in Grafana UI by clicking on it.

Log line search: only in body field.
Special case for traceID and spanID with additional search in traceID and spanID fields.

Labels extraction: handle first `| json |` explicitly, offloading to underlying log storage
where reverse mapping should be executed.

Extracted labels after `json`:
```
body: "hello world"
trace_id="cacb16ac5ace52ebe8749114234a8e58"
span_id="36a431d0481b2744"
severity="INFO"
http_duration=1105000000
http_duration_seconds=1.1054
http_method="GET"
http_server=true
http_status_code=200
http_user_agent=test-agent
```

So we can have a query like this:

```
{service_name="testService"} ~= `hello world` | json | http_duration > 30s or http_status_code >= 500
```

Which can be translated to efficient ClickHouse query.