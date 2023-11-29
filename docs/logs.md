# Logs model

## OpenTelemetry

The OpenTelemetry log model can be marshalled as JSON entry:

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