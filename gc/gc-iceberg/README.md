# Nessie Iceberg GC Notes

## Spark config

* Must use a separate branch (`.ref` Nessie catalog config)
* Must use the `NessieIcebergGcSparkCatalog` ?????????????
* Must use the `NessieIcebergGcSparkSessionCatalog`
* Explain the two Iceberg procedures

## TODOs

* migrate to spark().logXyz() functionality instead of slf4j ??
* GC-branches:
  * document why there are two
  * document why a GC-identify branch is recommended (and must be manually created)
  * document why a GC-expire branch is required
* Document workflow
  1. Run "identify" procedure
  2. Run "expire" procedure
