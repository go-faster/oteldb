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
