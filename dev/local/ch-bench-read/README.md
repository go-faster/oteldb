# Read benchmarks

To run benchmarks, use `run.sh` script.

# Comparing changes

Checkout base commit or `git stash push` current changes.

### Run benchmark on base

```console
$ cd dev/local/ch-bench-read
$ ./run.sh
```

Then save result in benchstat format:

```console
$ go run github.com/go-faster/oteldb/cmd/otelbench promql analyze -f 'benchstat' -i report.yml | tee bench.old.txt
```

### Run benchmark on changes

```console
$ ./run.sh
$ go run github.com/go-faster/oteldb/cmd/otelbench promql analyze -f 'benchstat' -i report.yml | tee bench.new.txt
```

### Compare results

```console
$ benchstat old=bench.old.txt new=bench.new.txt
```
