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
