# Changelog

## [0.1.0] - 2026-11-13

First standalone release, split from parent [dagster-object-factory](https://github.com/mikeblackett/dagster-object-factory.git)

## 0.2.0 - 2026-11-25

Refactor to support xclim v1.0.0

## 0.2.1 - 2026-11-25

Various bug fixes:

- Moves `xclim.set_options(as_dataset=False)` from main entry point to
  the component's execute function to prevent downstream changes;
- Fixes URLs with https protocol not being correctly formatted in
  metadata;
- Validates resampling partition keys with `xclim.parse_offset` for
  definition-time failure and consistency with the component's validation
  procedure.
