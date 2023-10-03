# Architecture

## Multi-tenancy

### Tenant

Tenant isolates resources from each other.

The tenant is defined as f(Resource) → id, where `id` is an integer.

The simplest case is when resource has attribute with tenant id,
like `org.go-faster.oteldb.tenant_id` integer.

TODO: define attribute.

## Partition

Data can be partitioned by tenant id and time.

```
Having:
 1) closed data partitioned by day (Δ=1D)
 2) active attributes and resources partitioned by hour (δ=1H)

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

/metrics/tenant/active/{attributes, resources}/*:
  dynamic tables of 2Δ data that is partitioned by δ (1H)
/metrics/tenant/points:
  dynamic table that stores 2Δ (2D) of point data

Each Δ:
1) Create new directory in /metrics/tenant/closed:
  e.g. /metrics/tenant1/closed/2021-01-01
2) Copy [T-2Δ, T-Δ) data from active/points to points static table
3) Merge [T-2Δ, T-Δ) data from active/attributes/* to attributes static table
4) Merge [T-2Δ, T-Δ) data from active/resources/* to resources static table
5) Delete data that is older than T-Δ from active tables
```
