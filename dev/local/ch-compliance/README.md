# PromQL Compatibility testing

https://github.com/prometheus/compliance/tree/main/promql#promql-compliance-tester

To build and install:
```
cd ./compliance/promql && go install ./cmd/promql-compliance-tester && cd -
```

To run with targets in docker-compose:
```console
promql-compliance-tester -config-file promql-test-queries.yml -config-file test-oteldb.yml
```

**NOTE:**
Results will be false-positive until enough data (~20min) is gathered.

This check was disabled as being broken on latest prometheus reference:
```yaml
# label_replace fails when there would be duplicated identical output label sets.
# !!! HACK: This was disabled because current prometheus reference implementation does not fail !!!
- query: 'label_replace(demo_num_cpus, "instance", "", "", "")'
  should_fail: true
```
Was producing an error:
```
FATA[0000] Error running comparison: expected reference API query "label_replace(demo_num_cpus, \"instance\", \"\", \"\", \"\")" to fail, but succeeded  source="main.go:137
```

Latest result:
```
Total: 547 / 548 (99.82%) passed, 0 unsupported
```