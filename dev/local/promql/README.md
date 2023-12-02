# PromQL Compatibility

This check was disabled as being broken on latest prometheus reference:
```yaml
# label_replace fails when there would be duplicated identical output label sets.
# !!! HACK: This was disabled because current prometheus !!!
- query: 'label_replace(demo_num_cpus, "instance", "", "", "")'
  should_fail: true
```
Was producing an error:
```
FATA[0000] Error running comparison: expected reference API query "label_replace(demo_num_cpus, \"instance\", \"\", \"\", \"\")" to fail, but succeeded  source="main.go:137
```

```json
547 / 547 [--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------] 100.00% 928 p/s
--------------------------------------------------------------------------------
QUERY: demo_memory_usage_bytes offset -1m
START: 2023-12-02 17:05:50.260327255 +0000 UTC, STOP: 2023-12-02 17:15:50.260327255 +0000 UTC, STEP: 10s
RESULT: FAILED: Query failed unexpectedly: execution: make range query: negative offset is disabled
--------------------------------------------------------------------------------
QUERY: demo_memory_usage_bytes offset -5m
START: 2023-12-02 17:05:50.260327255 +0000 UTC, STOP: 2023-12-02 17:15:50.260327255 +0000 UTC, STEP: 10s
RESULT: FAILED: Query failed unexpectedly: execution: make range query: negative offset is disabled
--------------------------------------------------------------------------------
QUERY: demo_memory_usage_bytes offset -10m
START: 2023-12-02 17:05:50.260327255 +0000 UTC, STOP: 2023-12-02 17:15:50.260327255 +0000 UTC, STEP: 10s
RESULT: FAILED: Query failed unexpectedly: execution: make range query: negative offset is disabled
================================================================================
General query tweaks:
None.
================================================================================
Total: 544 / 547 (99.45%) passed, 0 unsupported
```