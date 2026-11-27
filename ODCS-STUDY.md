# ODCS boundary study for #13404

Design/test-only artifact on SeaTunnel `39c74ce48b0fb5e0318326b8d57a42c7380c40c8` (dev, 3.1.1-SNAPSHOT). No runtime hook, public API, parser dependency, connector behavior change, or implementation PR is proposed by this branch.

## Observation point and caller result

For the inspected Zeta path, `MultipleTableJobConfigParser` obtains transform outputs at line 559, routes each table to `createSinkAction` at line 775, and calls `FactoryUtil.createAndPrepareSink` at line 815. That method creates `TableSinkFactoryContext` at line 246 before calling the sink factory. `replacePlaceholderAndCreate` rewrites configuration placeholders but passes the same `CatalogTable` into the context.

The proposed T observation is an attribute snapshot of the per-table input to `createAndPrepareSink`, before the factory runs. It is not a hook installed by these tests. This covers the traced Zeta factory route, not the legacy fallback, dry-run sample branch, every execution engine, or later CDC schema events. A later implementation must settle those entry points independently.

`JdbcSinkFactoryTest` constructs real `CatalogTable` and `TableSinkFactoryContext` objects. The factory probe calls the real `JdbcSinkFactory` without opening a database. It records the incoming table separately from `getWriteCatalogTable()`: database/table overrides, the existing deprecated `tablePrefix`/`tableSuffix` options, and configured primary-key order change the latter. The incoming schema snapshot remains unchanged. This is evidence that one must not label the factory output as the original T.

The test-only output is a JSON-compatible attribute map, serialized as one `ODCS_STUDY` line per observation. It is a sketch of a caller result, not a new return type for the existing void dry-run SPI. Schema observations retain:

- Result-format version and pinned SeaTunnel source revision.
- ODCS standard version, contract ID/version, and SHA-256 of exact inline contract bytes.
- Exact-case names, declared-field presence, extra-field policy, and producer-to-consumer decimal policy.
- Boundary, copied table/field/type/precision/scale/nullability/key attributes.
- Individual reason/field/expected/observed findings; scalar-subset outcome; explicitly unevaluated capabilities; separate actual-target outcome.

The contract is manually mapped. No general ODCS parsing or schema-validation claim is made. The standard version is `v3.2.0`; contract `orders-study` version `1.0.0` has exact-byte SHA-256 `b8656bc586b1e7a18804020b2b7bea54a7d008d7697e154fab27c5f1f470d6ef`. JSON whitespace changes that digest. A new test-only identity comparison keeps different digests under the same ID/version visible as a validation error; it is not an identity registry or runtime admission rule.

## Executed observations

| Group | Fixtures | Observed result |
| --- | --- | --- |
| Bounded scalar schema | Match; narrower decimal; integer overflow; scale overflow; extras allowed/rejected; missing nullable field; rename; required-to-nullable; table/column case; key order; wrong type family; conflicting decimal metadata; non-null nullable field | 15 named observations with asserted outcomes/reasons. Conflicting redundant decimal metadata is unsupported, not arbitrarily resolved. |
| S versus T | Two constructed pairs, four observations | `(18,2)` fails while `(18,4)` fits; `(16,2)` fits while `(18,2)` fails. These are schema fixtures, not executed transforms or row-preservation evidence. |
| Factory output | Incoming versus configured sink-write table/key | Real factory changes destination and ordered key; preserves incoming snapshot. |
| Planned decimal SQL | Typed `(18,4)`; explicit sink `(18,2)`; preserved MySQL source `(16,2)`; typed `(70,35)` | Real builder emits `(18,4)`, `(18,2)`, `(16,2)`, `(65,30)` respectively. |
| Planned nullable key SQL | Index generation disabled/enabled | Real builder emits NULL/NOT NULL respectively, while input metadata stays nullable. |

The original study produced 26 observation records. The follow-up adds seven contract-input records: logical number without physical decimal metadata, missing key position, duplicate key position, nested array, quality rule, SLA, and same-ID/version content conflict. The first six retain their actual unsupported requirement and reason in serialized output; the last preserves both exact-byte digests and a validation-error outcome. They project only known requirements from constructed JSON fixtures, not arbitrary ODCS documents. A separate aggregation test specifies that validation errors and known mismatches take priority, unsupported requirements cannot disappear into a compatible result, and an empty observation set is unsupported.

These are 33 observation records, not independent test methods. The two selected test classes now contain 35 tests including existing tests. Formatting and the JDBC reactor `verify` are run on JDK 11; the module's integration tests are skipped by its existing build configuration. No database or end-to-end CDC test is claimed.

Reproduce from this branch with the project's supported Maven installation and JDK 11:

```sh
mvn -B -ntp -pl seatunnel-connectors-v2/connector-jdbc -am \
  spotless:apply verify \
  -Dtest=JdbcSinkFactoryTest,MysqlCreateTableSqlBuilderTest \
  -Dsurefire.failIfNoSpecifiedTests=false
```

`-am` with `verify` is intentional: the upstream config shade module must reach package before downstream classes use its shaded types. Local verification used a Central-only settings file for dependency resolution; no repository dependency configuration was changed.

## Actual target D is a separate observation

`JdbcSink.getDatabaseTableSchema` at line 225 shows the catalog lookup: resolve the destination/identifier policy, open the selected catalog, and call `getTable`. A future observer should record the actual table, read time and catalog provenance without invoking save-mode handlers. Missing table, missing catalog support and catalog-read failures are different outcomes. A failure must not become a successful comparison.

The SQL-builder observations are labeled `planned-ddl-only`, with `contractEvaluation=not-performed` and `actualTargetOutcome=unsupported / TARGET_NOT_OBSERVED`. They are independent native-type probes, not contract comparisons. No CREATE/DROP/DML was executed. In particular, neither the in-memory type nor generated SQL proves what the database currently stores or will accept.

## Limits and decisions still required

- Scalar compatibility is not whole-contract compatibility. String character capacity remains unevaluated because `Column.columnLength` describes bytes, not a portable character count. Unsigned bounds, timezone/charset/collation, nested types, quality/uniqueness and SLA remain unsupported. The fixture key comparison does not prove data uniqueness.
- The proposed aggregation is: validation errors remain distinct; a known mismatch wins over unsupported; otherwise any unsupported obligation yields unsupported; only a fully evaluated declared subset can be compatible. The test-only reducer specifies those priorities but does not connect a production whole-contract evaluator.
- Logical-number-only, identity conflict, missing/ambiguous key positions, nested array, quality and SLA inputs now have dedicated fixtures. Their shapes were checked against the pinned ODCS v3.2.0 schema definitions (`f0bdad95346905d500be5ef4b2c2d9b1d95223b7`); no schema validator or general parser was run. This is not a claim that every row or every possible input in the earlier conceptual matrix is implemented.
- The maps are mutable test fixtures, not a production immutable result API. Before implementation, settle immutable ownership, per-obligation paths/reason codes, and whether the existing dry-run reporting layer can carry results without changing its SPI.
- No automatic enforcement, checkpoint/recovery semantics, remote registry, or pre-DDL safety guarantee is established. Do not infer these from successful unit tests.

Next design decision: agree on the attribute-snapshot/result contract and a read-only existing-target observer before adding any parser or runtime enforcement.
