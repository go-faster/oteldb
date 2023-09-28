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

## Partitioning and sharding

Data should be sharded by time (e.g. per day, week or similar, can be heterogeneous) and tenant id.

For example:

```
/metrics
  /tenant01
     /2021-01-01 <- time shard
       values
       attributes
       resources
    /2021-01-02
  /tenant02
    /2021-01-01
```

Query can be efficiently executed concurrently.
Also, there are concurrent `LogQL` executors that can exploit that.

##

## Query execution

### Planning

We should select all shards that fit query time range and tenant id.
On this stage we should map tenant label (resource attribute) set to tenant id.

Multi-tenant queries are possible.

Artifact: list of shards with metadata about tenant id and time range.
