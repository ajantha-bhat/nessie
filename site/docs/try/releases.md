# Releases

**See [Nessie Server upgrade notes](server-upgrade.md) for supported upgrade paths.**

## 0.46.0 Release (December 20, 2022)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.46.0).

* Make generated OpenAPI type names more readable (#5732)
* Add serialized form tests for `CommitMeta` v2 (#5731)
* Add v2 attributes to `CommitMeta` (#5706)
* CI: No duplicate checkstyle task runs (#5703)
* Allow publishing Gradle build scans from CI (#5701)
* Remove legacy Spark extensions (#5704)
* Fix flaky TestKeyGenerator (#5702)
* Support merge/transplant message overrides (#5686)
* Add common ancestor commit in merge tests (#5696)
* Add global test timeout for versioned/persist/ (#5690)
* Add DeleteContent command (#4773)
* Expose updated reference data in java client APIs (#5670)
* Fix OpenAPI docs for the required "expected hash" parameters (#5665)
* Expose MergeBehavior to java clients (#5682)
* Expose `CommitResponse` in java client API (#5673)
* Migrate `FetchOption` to the `model` package (#5667)
* Add README.md to model (#5668)
* Use API v2 in GC CLI (#5660)
* Add pagination params to the Diff API v2 (#5452)
* Replace deprecated native build parameters (#5662)
* Move Graal/native registration to nessie-quarkus-common (#5661)
*   Increase gradle memory in integration tests (#5654)
* Remove unused Jaeger properties (#5647)
* Validate ID for explicitly created Namespaces (#5644)
* Gradle: no longer run test classes concurrently (#5653)
* Nit: remove TODO (#5652)
* Fix GetReferenceParams description for API v2 (#5639)
* Import: move repo-setup part to nessie-versioned-transfer (#5640)
* Export using reference/commit-log scanning (#5635)
* Switch nessie-client to use OpenTelemetry (#5607)
* Version-store tests: use proper Put operations (#5614)
* DatabaseAdapter/identify-heads-and-forks: Allow commit-log scanning (#5638)
* Version-store tests: Adjust test to currently undetected transplant conflict (#5613)
* DatabaseAdapterExtension: Allow adapter-configurations on fields (#5637)
* Export: store exporting Nessie version and show during import (#5636)
* Adopt REST tests to stricter validations (#5612)
* PersistVersionStore: expose missing additional-parents (#5633)
* Export/import: explicit tests for exporter/importer implementations (#5631)
* DatabaseAdapter/identifyHeadsAndForks: Don't return `NO_ANCESTOR` as fork point (#5632)
* Export/import: remove unused functionality from BatchWriter (#5630)
* Rely on new `NessieVersion` in CLI tools (#5629)
* Allow `-` as a reference name in v2 REST paths (#5618)
* Export/import: move file related code to separate package (#5628)
* Use soft-assertions in jaxrs-tests (#5610)
* Adopt versioned-tests to not set cid for "new" `Content`; minor refactoring for merge/transplant (#5611)
* Deprecate commit lists in merge-results (#5599)
* Add Nessie version into nessie-model.jar (#5609)
* Minor verstion-store commit-log test refactoring (#5604)
* Minor merge test enhancements (#5603)
* Add "diff test" with a key "in between" (#5602)
* Add a some test cases for "reference not found" scenarios (#5601)
* Factory methods for EntriesResponse.Entry + DiffResponse.Entry + MergeResult.KeyDetails (#5600)
* Add expected content to v2 REST update namespace (#5598)
* Switch to OpenTelemetry in Nessie Quarkus server (#5605)
* Minor code clean-up in client-side Namespace operations (#5594)
* Fix v2 URL path mapping for getReferenceByName (#5596)
* Add REST API v2 (#5004)
* Fix `@Parameter` annotations in `DiffParams` (#5589)
* Build UI against Nessie API v1 from 0.45.0 (#5587)

## 0.45.0 Release (November 29, 2022)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.45.0).

* Fix OpenAPI spec for DiffApi (#5584)
* Content generator: allow functional key patterns and more (#5575)
* CI: auto-destruct spawned Nessie Quarkus runner JVM (#5583)
* Scala/Gradle: keep Scala compiler alive, bump Scala 2.13 to 2.13.10 (#5576)
* Fix BaseExceptionMapper's NPE when an Exception's message is null (#5563)
* Add RefreshContent command (#5551)
* Fix erasing repo descriptions (#5557)
* Perftest: allow passing system properties to launched Nessie (#5556)
* Transfer: completely abstract `DatbaseAdapter` from core export/import (#5555)
* Remove dependency to database-adapter type from `ImportResult` (#5553)
* Deprecate `VersionStore.getRefLog()` (#5554)
* Nessie export/import: update abstraction for file-typed stuff (#5545)
* Nessie CLI: rename `call()` to `callWithDatabaseAdapter()` (#5540)
* Nessie import: check export version (#5543)
* Nessie CLI: Update wording for in-memory warning (#5542)
* Nessie/Quarkus: use `Instance<DatabaseAdapter>` (#5541)
* Update verification code for Nessie CLI erase-erpository (#5539)
* Add content-info Quarkus CLI command (#5501)
* Reduce memory pressure during Quarkus CLI integration tests. (#5530)
* Bump+change Quarkus builder image to Mandrel + 22.3 (#5526)
* renovate: put python back on weekly schedule (#5519)
* Docs: Update Spark Python/Java API docs (#5517)
* Fix latest Nessie and Iceberg versions in side docs (#5499)
* Refactor Nessie-Jax-RS extension a little bit (#5486)
* renovate: fix fileMatch regexp (#5489)
* fix renovate python requirements file pattern (#5481)
* Proper put-update-operation for namespace-update (#5484)
* Move tests from o.p.jaxrs to o.p.jaxrs.tests (#5483)
* Fix a few checkstyle warnings (#5482)
* Update protobuf plugin to 0.9.1 (#5457)
* Documentation accommodating new version format of Iceberg and Nessie artifacts (#5371)
* Pull `ProtobufHelperPlugin` from `gradle-build-plugins` (#5456)
* Refactor some test code to soft-assertions (#5446)
* Move Merge/Transplant classes to the api.v1.params package (#5435)
* Move old API classes into the api.v1 package (#5419)
* Move some tests from db-adapter test code to verstion-store test code (#5445)
* Support method-level NessieApiVersions annotations (#5436)
* Introduce multi-version API test framework (#5420)
* Extract service-side interfaces for RefLog and Namespace services (#5418)
* Extract common java client builder code (#5411)
* Extract service-side interfaces for client-facing services (#5412)
* Remove OpenAPI spec properties from build script. (#5413)
* Build UI against Nessie API v1 from 0.44.0 (#5408)
* Refactor TestAuthorizationRules (#5399)
* Java 11 HttpClient (#5280)
* Testing/nit: Logging for :nessie-s3minio ITs (#5398)
* Add non-trivial tests for assign branch/tag operations (#5395)
* Fix version in the Nessie Helm Chart (#5392)
* Extract a multi-env test engine into a module. (#5339)
* Move Nessie Client construction into a JUnit5 extension (#5370)
* Record next development version (#5387)
* GH workflows: Add missing `cache-read-only: true` (#5385)
* GH create release WF: next version not properly recorded (#5386)

## 0.44.0 Release (October 18, 2022)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.44.0).

* Ninja: fix create-release WF
* Ninja: fix GH env reference
* Update README mentioning Iceberg 1.0 (#5384)
* Add Dan + Vladimir to devs list (#5381)
* GH release WF: default to "minor" version bump (#5380)
* GH release WF: Fix wrong task name (#5379)
* Automatically update release text files (#5377)
* GH release WF - fix log exclusion + log filter for rel-notes (#5378)
* Nessie GC: Docs (#5209)
* Nessie GC: Command line tool (#5227)
* Nessie GC: Iceberg functionality (#5207)
* Avoid direct dependency on iceberg-bundled-guava (#5366)
* quarkus-server tests use dynamic port from env (#5352)
* Fix missing placeholder for Preconditons.checkState (#5360)
* fix GH workflows still mentioning maven (#5353)
* Remove httpClient param from AbstractRest.init (#5354)
* Update pretty-ms to 8.x (#5341)
* DynamoDB related test changes (#5338)
* Split unsquashed merge tests into dedicated test methods (#5328)
* Update testing-library-react to 13.x (#5334)
* Bump actions/checkout from v3 to v3.1.0 (#5335)
* Update material-ui to mui 5.x.x (#5326)
* nessie-client/test: compress responses for all relevant HTTP methods (#5323)
* Fix micrometer path replacement patterns (#5321)
* Renovate: labels for java/javascript/python (#5318)
* Revert protobuf to 3.21.6 (#5317)
* Renovate: limit to 2 PRs per hour (#5319)
* Isolate http-level test from java client-level tests (#5314)
* Skip newer-java workflow on forks (#5294)
* Nessie client tests: Replace JDK's HTTP server w/ Jetty (#5285)
* Move internal classes of the Nessie HTTP client (#5286)
* GH/WF: Use `temurin` instead of 'zulu` (#5289)
* Nessie-client tests: do not compile w/ older Jackson versions (#5287)
* Test Nessie client with Java 8 (#5284)
* Integrate Jackson-version tests into Gradle build (#5279)
* Migrate to Gradle version catalogs (#5167)
* Unify Postgres container version declaration for tests
* Migrate from dependabot to renovate (#5166)
* Testing pre-requisites for Nessie GC: Two S3 testing projects (#5142)
* Fix iceberg verison on web site (#5222)
* Java 19 testing (#5221)
* Automatic patch releases (#5214)
* Unsupport 0.30.x versions (#5212)
* WF: Remove Maven part (#5213)
* Nessie GC: JDBC Live-Set-Repository (#5208)
* Allow Hadoop Spark config in tests (#5206)
* Nessie GC: mark & sweep (`gc-base` module only) (#5144)
* Slight build scripts change for Java11+ target compat (#5203)
* Fix test failure caused by #5147 (#5204)
* No longer write ref-log entries for commit/merge/transplant (#5147)
* Schedule dependabot for npm + pip less frequently (#5201)

## 0.43.0 Release (September 15, 2022)

**Rolling upgrades from versions before 0.40.0 are not supported!**

* Prepare for Nessie GC
* Nessie export/import functionality
* Use Graal 22.2-java17 for native images
* Several test and build improvements

## 0.42.0 Release (August 26, 2022)

**Rolling upgrades from versions before 0.40.0 are not supported!**

* Key list creation fixes
* Pluggable content types

## 0.41.0 Release (August 5, 2022)

**Rolling upgrades from versions before 0.40.0 are not supported!**

* Do not persist and expose attached content metadata
* Fix issue when looking up key in an open-addressing-key-list

## 0.40.3 Release (August 1, 2022)

**Rolling upgrades from versions before 0.40.0 are not supported!**

* Remove Quarkus-BOM dependency from non-Quarkus projects

## 0.40.2 Release (August 1, 2022)

**Rolling upgrades from versions before 0.40.0 are not supported!**

* Dependency issues fixed

## 0.40.1 Release (July 25, 2022)

**Rolling upgrades from versions before 0.40.0 are not supported!**

* Fix key-lists issue resulting in server-errors (`ArrayIndexOutOfBoundsException`)

## 0.40.0 Release (July 22, 2022)

**Rolling upgrades from earlier versions are not supported!**

* Support Spark 3.1 (Scala 2.12), Spark 3.2 (Scala 2.12 + 2.13), Spark 3.3 (Scala 2.12 + 2.13)
* Support Iceberg 0.14.0
* Nessie Spark SQL extensions: handle timestamps w/ time-zones
* Nessie Spark SQL extensions: fix handling of `USE`d references for `CREATE/ASSIGN BRANCH/TAG`
* Detailed merge/transplant result to allow inspection of conflicts
* Merge/transplant optionally allow "force-keep" & "force-merge" of conflicting content-keys
* Iceberg table metadata stored in Nessie
* Improvements to REST error handling
* Performance improvements when there are many content-keys
* Hard limit on content-key length (max 20 elements, total 500 characters)
* Prevent (accidental) deletion of default branch
* Improved usage of automatic paging via `NessieApi`
* Improvements to Nessie server health checks
* Add rolling-upgrade test suite in regular CI
* Daily testing against Java 17 + newer
* Switched to Java 17 in native images
* Build switched from Maven to Gradle

## 0.30.0 Release (May 13, 2022)

* Add commit-ID to KeyListEntry when writing new key-lists
* Do not process old key-lists when retrieving values
* Helm: Fix k8s version detection in ingress template
* Database-adapter: commit optimizations
* Remove the configurable default for the configurable values for getDefaultMaxKeyListSize
* Dynamo/Mongo/TX: use bulk/batch requests where possible

## 0.29.0 Release (May 5, 2022)

* Spark SQL: Configure ref.hash for NessieCatalog only when explicitly requested
* Escape all column names in SQL DML+DDL statements
* Use hashOnRef when fetching Namespaces
* Helm: Add ingress support for Kubernetes >=1.22
* Fix CockroachDB transaction-retry behavior

## 0.28.0 Release (April 26, 2022)

* Generate unique content IDs for explicitly created namespaces
* Fix patterns for metrics
* Various test improvements (CI + build)
* Various minor code fixes (fixes for errorprone warnings)

## 0.27.0 Release (April 14, 2022)

* Support for Namespace properties
* Make NessieContentGenerator extensible

## 0.26.0 Release (April 12, 2022)

**Rolling** upgrades from an older Nessie release to 0.26.0 or newer are not supported.

* Remove global state for Iceberg tables + views
* Internal optimizations in database adapters, version store and API endpoints
* Change 'marker' character to indicate `.` in namespace/table identifiers from ASCII 0 to `\u001D`
* Opt-in to force-merge or not merge specific content keys (also for transplant)
* Squash merged and transplanted commits by default (with opt-out)

## 0.25.0 Release (April 6, 2022)

* Nessie Quarkus Server can use Postgres as its backend database
* Explicitly define behavior of multiple commit-operations in a commit
* Load correct view metadata for a given ref

## 0.24.0 Release (March 31, 2022)

* Prevent explicit creation of empty namespaces
* Add content-id to `BatchAccessChecker.canReadContentKey()`

## 0.23.1 Release (March 23, 2022)

* Support Namespaces
* CI "perf tests" improvements
* SQL Extension: Fix Create reference from a hash on non-default reference
* Enhance authorization checks
* Support custom annotations on Nessie Helm service

## 0.23.0 Release (March 23, 2022)

(not properly released)

## 0.22.0 Release (March 11, 2022)

* Improve performance of `getValues`
* Global-log compaction
* Store-level maintenance CLI
* Reduce number of tags for micrometer
* Grafana Dashboard for Nessie service
* Add new commands to generate-content tool

## 0.21.2 Release (March 2, 2022)

* Fix serialization issue

## 0.21.1 Release (March 2, 2022)

* (no user visible changes)

## 0.21.0 Release (March 1, 2022)

* Add tracing to database-adapter internals
* Introduce compatibility and upgrade tests
* Refactor StreamingUtil class
* Support for Spark 3.1 + 3.2.1 in Nessie SQL extensions
* Proper usage of commit-id in Spark SQL extensions
* Add DELETE_DEFAULT_BRANCH access check

## 0.20.1 Release (February 17, 2022)

* (no user visible changes)

## 0.20.0 Release (February 16, 2022)

* Enable metrics for MongoDB by default
* Make try-loop-state configurable and add metrics
* Reorganize routes in UI
* Improve error reporting in Nessie Java client
* Various test improvements

## 0.19.0 Release (February 7, 2022)

* Reads using "detached" commit-ids w/o specifying a branch or tag name
* Bump Nessie client version in Nessie Spark-Extensions
* Support for Iceberg views (experimental)
* Diff endpoint supports named-references + commit-on-reference as well
* Add filtering for ref-log
* Rework and simplification of the Nessie UI code

## 0.18.0 Release (January 13, 2022)

* Add reflog support
* Uses commit-timestamp "now" for merged/transplanted commits
* Add new reflog command to the CLI
* Add support for Python 3.10
* Drop support for Python 3.6

## 0.17.0 Release (December 08, 2021)

* Rename 'query_expression' query param to 'filter'
* Rename 'max' query param to 'maxRecords'
* Rename 'fetchAdditionalInfo' query param to 'fetch' for better extensibility

## 0.16.0 Release (December 03, 2021)

* Mark optional fields as @Nullable / add validation for required fields in param classes
* Add CEL-filter to get-all-references
* Fix NPE for unchanged operation for fetching commit log with additional metadata
* Allow CEL-filtering on optional operations in get-commit-log

## 0.15.1 Release (December 01, 2021)

* Fix wrongly placed validation annotation

## 0.15.0 Release (December 01, 2021)

* Enhance commit log to optionally return original commit operations
* Optionally return commits ahead/behind, HEAD commit-meta, commit count,
  common ancestor for named references
* Add missing REST endpoint to retrieve diff between two references
* Web UI improvements

## 0.14.0 Release (November 12, 2021)

* Updated `IcebergTable` to track more information
* UI dependencies cleanup
* OpenAPI/REST API cleanup (breaking change)

## 0.12.1 Release (November 3, 2021)

* Test code improvements
* Swagger examples fixes
* Web UI improvements
* Faster local builds w/ `./mvnw -Dquickly`

## 0.12.0 Release (October 25, 2021)

* Specialize and document Nessie exceptions
* Adopt Helm chart with new Nessie server settings
* Bump to GraalVM 21.3

## 0.11.0 Release (October 20, 2021)

* Various doc + site improvements
* Fix Nessie's representation of global and on-reference state (Iceberg tables)
* Fix CLI log -n option
* Spark SQL extension improvements

## 0.10.1 Release (October 8, 2021)

* Spark SQL extension changes
* Various (Open)API and client (Java, Python) implementation changes to prepare for better
  backwards compatibility.
* JUnit extension based test support against different database/store types and configurations
* Unified version-store implementations into a part w/ the Nessie logic and a tier implementing
  database access (MongoDB, DynamoDB, RocksDB, PostgreSQL).
* Remove JGit

## 0.9.2 Release (August 26, 2021)

* Cleanup & fixes to OpenAPI examples, for Swagger UI
* Update Deltalake client to use version 1.0.0
* Drop Deltalake support for Spark 2
* Remove Hive-Metastore bridge
* Preparations for backwards-compatible Client-API
* Spark SQL Extensions: Introduce `IF NOT EXISTS` for `CREATE BRANCH`/`CREATE TAG`
* Spark SQL Extensions: Updates to work with Iceberg 0.12.0

## 0.9.0 Release (August 9, 2021)

* Support for the upcoming Iceberg `0.12.0` release for both Spark 3.0 + 3.1
* Add docs for Nessie's metadata authorization
* Add SPI for Nessie authorization with Reference implementation
* Create Helm chart for Nessie

## 0.8.3 Release (July 19, 2021)

* Fix issue in spark sql extensions
* Python CLI: Fix ser/de of DeltaLakeTable when listing contents

## 0.8.2 Release (July 15, 2021)

* Add JAX-RS server implementation based on Glassfish/Jersey/Weld for integration testing
  in Iceberg
* REST-API change: only accept named-references
* REST-API change: support time-travel on named-references
* REST-API change: Server-side commit range filtering
* OpenAPI: more explicit constraints on parameters
* OpenAPI: include OpenAPI yaml+json files in nessie-model artifact
* Remove already deprecated methods from ContentsApi
* Commit-log filtering on all fields of CommitMeta
* Use "Common Expression Language" for commit-log and entries filtering
* Spark-extensions for Iceberg
* Prepare for multi-tenancy
* Gatling support + simulations
* Python CLI: Fix ser/de of DeltaLakeTable when listing contents

## 0.7.0 Release (June 15, 2021)

* Server-side filtering improvements for entries-listing and log-listing
* Distinguish between author & committer in the Python CLI
* Allow setting author when committing via Python CLI
* Loosen pins for client install on Python cli
* Fix edge case when merging using in memory + jgit stores
* Gradle plugin improvements
* (Development) change to Google Code Style, add spotless plugin
* (CI) Add OWASP Dependency Check

## 0.6.1 Release (May 25, 2021)

* Gradle plugin improvements

## 0.6.0 Release (May 12, 2021)

* TreeApi.createReference() + commitMultipleOperations() return commit information
* Iceberg GC actions and a process to execute GC algorithm

## 0.5.1 Release (April 9, 2021)

* Fix Gradle plugin (non-deterministic order of dependencies causing failures)
* Fix Web-UI

## 0.5.0 Release (April 8, 2021)

* Iceberg table GC support
* Consistency fixes under high load
* Breaking changes to the backend to support richer commit metadata and data types
* Performance, metrics and tracing improvements
* Gradle plugin improvement for incremental builds

## 0.4.0 Release (March 8, 2020)

* rename base package to org.projectnessie
* NessieClient is now an interface and some easier builders
* initial implementation of GC algorithm
* major refactor of tiered classes for better modularity and extensibility
* observability improvements including better DynamoDB metrics and opentracing support for the client

## 0.3.0 Release (December 30, 2020)

* 118 commits since 0.2.1
* Replace jax-rs client with one based on HttpURLConnection
* Update Quarkus to 1.10.5
* Improvements to Server including better UI routing, validation checks on inputs etc
* Various improvements to python client and cli. Including python3.9 support

## 0.2.1 Release (October 30, 2020)

* Fix missing dateutil requirement for pynessie install
* Address path discovery in Gradle plugin (for testing in external integrations)

## 0.2.0 Release (October 29, 2020)

* Update [Nessie CLI](../tools/cli.md) commands to better match `git` syntax
* Update [REST Apis](../develop/rest.md) to be more consistent and better
* Add support for merge & cherry-pick in DynamoDB storage backend
* Add [WebUI](../tools/ui.md)
* Introduce new DynamoDB optimizations to support faster log and entry retrieval
* Update to Quarkus 1.9.1
* Expose the new [Store interface](https://github.com/projectnessie/nessie/blob/main/versioned/dynamodb/src/main/java/org/projectnessie/versioned/store/Store.java) for low level storage implementations
* Introduce Quarkus Gradle runner plugin for easier third-party testing (e.g. Iceberg)
* Enable [swagger-ui](../tools/ui.md) by default in Nessie service

## 0.1.0 Release (October 1, 2020)

* Initial release
