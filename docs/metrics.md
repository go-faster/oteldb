# Metrics

## Query language

Using `PromQL`.

## Mapping

The `PromQL` does not distinguish between `attributes` and `resource`.
Also, mapping is required for attribute names and values.

1. Limitations on label name, dots are not supported
2. All label values are just strings

## Stateful mapping

To support unambiguous mapping in both directions, some state should be maintained.

During query execution, query engine should fetch all resource and attribute sets from the storage
that fits query within requested time range.

This can be achieved by maintaining index for resources and attributes keys in the storage, like that:

```
/resource:   | key | value |
/attributes: | metric | key | value |
```

The resource is not directly connected to metric name and potentially has lower cardinality
in single time shard, so we don't add metric name to key.

### Cardinality

1. Metric name
2. Resource set
3. Attribute set

Data should be stored in a way that allows exploit low cardinality of metric name and resource set,
subsequently reducing series cardinality.

However, we can benefit of filtering concurrently by attribute and resource set,
maintaining index for resources and attributes.

Also, actual cardinality of resource set can be smaller than metric cardinality
locally in single time shard because of churn rate.

## Attribute set hash

The `resource` attribute set and `attributes` set can be hashed to produce
fixed-size key for the index. That is, we can now store metric points like that:

```
| name | resource_hash | attribute_hash | timestamp | value |
```

Having effective joins with filtered `resource` and `attributes` tables
and primary sorting key `(name, resource_hash, attribute_hash, timestamp)` we can efficiently
fetch all points needed for query execution.

Probably we can replace hashes by integers, but this complicates implementation
of query ingestion where we should perform "insert or do nothing" operation with id lookup.

### Example

Having metric like that:

```yaml
name: http.request.duration
attributes:
  foo: 1
  bar: baz
resource:
  tenant.id: 1
  service.name: api
timestamp: 2021-01-01T00:00:00Z
value: 10
```

We compute hashes from attribute sets:

| name       | value                            |
| ---------- | -------------------------------- |
| attributes | 3b52e723db6a5e0aaee48e0a984d33f9 |
| resource   | 4bfe5ab10b4f64a45383b67c222d962c |

Attribute set is represented by an ordered list attributes to make hash deterministic.

#### `resource`

| key                              | value                                       |
| -------------------------------- | ------------------------------------------- |
| 4bfe5ab10b4f64a45383b67c222d962c | `{"tenant.id": "1", "service.name": "api"}` |

#### `attributes`

| metric                | key                              | value                      |
| --------------------- | -------------------------------- | -------------------------- |
| http.request.duration | 3b52e723db6a5e0aaee48e0a984d33f9 | `{"foo": 1, "bar": "baz"}` |

#### `points`

| name                  | resource_hash                    | attribute_hash                   | timestamp            | value |
| --------------------- | -------------------------------- | -------------------------------- | -------------------- | ----- |
| http.request.duration | 4bfe5ab10b4f64a45383b67c222d962c | 3b52e723db6a5e0aaee48e0a984d33f9 | 2021-01-01T00:00:00Z | 10    |

## Partitioning and sharding

Data should be sharded by time (e.g. per day, week or similar, can be heterogeneous) and tenant id.

Having:

1.  closed data partitioned by day (Δ=1D)
2.  active attributes and resources partitioned by hour (δ=1H)

```
/metrics
  /tenant-1
     /active
       points
       attributes/
         2021-01-01-T-20-00
         2021-01-01-T-21-00
         2021-01-01-T-22-00
         2021-01-01-T-23-00
       resources/
         2021-01-01-T-20-00
         2021-01-01-T-21-00
         2021-01-01-T-22-00
         2021-01-01-T-23-00
     /closed
       /2021-01-01
         points
         attributes
         resources
```

- `/metrics/tenant/active/{attributes, resources}/*`:

  dynamic tables of 2Δ data that is partitioned by δ (1H)

- `/metrics/tenant/points`:

  dynamic table that stores 2Δ (2D) of point data

Each Δ:

1. Create new directory in `/metrics/tenant/closed`:
   e.g. `/metrics/tenant1/closed/2021-01-01`
2. Copy `[T-2Δ, T-Δ)` data from `active/points` to `points` static table
3. Merge `[T-2Δ, T-Δ)` data from `active/attributes/\*` to `attributes` static table
4. Merge `[T-2Δ, T-Δ)` data from `active/resources/\*` to `resources` static table
5. Delete data that is older than `T-Δ` from active tables

Query can be efficiently executed concurrently.
Also, there are concurrent `LogQL` executors that can exploit that.

- https://github.com/thanos-io/promql-engine

## Query execution

### Planning

We should select all shards that fit query time range and tenant id.
On this stage we should map tenant label (resource attribute) set to tenant id.

Multi-tenant queries are possible.

Artifact: list of shards with metadata about tenant id and time range.
