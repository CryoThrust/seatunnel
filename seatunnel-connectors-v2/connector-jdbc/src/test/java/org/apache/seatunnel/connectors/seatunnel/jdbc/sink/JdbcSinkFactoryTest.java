/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.schema.SchemaChangeType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class JdbcSinkFactoryTest {

    // Test-only, manually mapped scalar contract. No ODCS parser or runtime hook is installed.
    private static final String ODCS_STUDY_CONTRACT =
            "{\"apiVersion\":\"v3.2.0\",\"kind\":\"DataContract\","
                    + "\"id\":\"orders-study\",\"version\":\"1.0.0\",\"name\":\"Orders study\",\"status\":\"draft\","
                    + "\"schema\":[{\"name\":\"orders\",\"physicalName\":\"orders\",\"logicalType\":\"object\",\"properties\":["
                    + "{\"name\":\"tenant_id\",\"logicalType\":\"integer\",\"physicalType\":\"BIGINT\",\"required\":true,\"primaryKey\":true,\"primaryKeyPosition\":1},"
                    + "{\"name\":\"order_id\",\"logicalType\":\"integer\",\"physicalType\":\"BIGINT\",\"required\":true,\"primaryKey\":true,\"primaryKeyPosition\":2},"
                    + "{\"name\":\"amount\",\"logicalType\":\"number\",\"physicalType\":\"DECIMAL(18,4)\",\"required\":true},"
                    + "{\"name\":\"note\",\"logicalType\":\"string\",\"physicalType\":\"VARCHAR(128)\",\"required\":false}]}]}";

    @Test
    void testOdcsStudyScalarObservations() throws Exception {
        assertOdcsStudy("matching", odcsColumns(18, 4), "orders", false, false, "compatible", "");
        assertOdcsStudy(
                "decimal-narrower", odcsColumns(16, 2), "orders", false, false, "compatible", "");
        assertOdcsStudy(
                "decimal-integer-overflow",
                odcsColumns(18, 2),
                "orders",
                false,
                false,
                "incompatible",
                "DECIMAL_INTEGER_CAPACITY");
        assertOdcsStudy(
                "decimal-scale-overflow",
                odcsColumns(16, 6),
                "orders",
                false,
                false,
                "incompatible",
                "DECIMAL_SCALE");

        List<Column> columns = odcsColumns(18, 4);
        columns.add(PhysicalColumn.of("extra", BasicType.STRING_TYPE, 128L, true, null, null));
        assertOdcsStudy("extra-allowed", columns, "orders", true, false, "compatible", "");
        assertOdcsStudy(
                "extra-rejected", columns, "orders", false, false, "incompatible", "EXTRA_FIELD");
        columns = odcsColumns(18, 4);
        columns.remove(3);
        assertOdcsStudy(
                "missing-nullable-field",
                columns,
                "orders",
                false,
                false,
                "incompatible",
                "MISSING_FIELD");
        columns = odcsColumns(18, 4);
        columns.set(
                1, PhysicalColumn.of("renamed_order", BasicType.LONG_TYPE, 19L, false, null, null));
        assertOdcsStudy(
                "renamed-required-field",
                columns,
                "orders",
                false,
                false,
                "incompatible",
                "MISSING_FIELD");
        columns = odcsColumns(18, 4);
        columns.set(1, PhysicalColumn.of("order_id", BasicType.LONG_TYPE, 19L, true, null, null));
        assertOdcsStudy(
                "nullable-required-field",
                columns,
                "orders",
                false,
                false,
                "incompatible",
                "NULLABILITY");
        assertOdcsStudy(
                "table-case-difference",
                odcsColumns(18, 4),
                "Orders",
                false,
                false,
                "incompatible",
                "TABLE_NAME");
        columns = odcsColumns(18, 4);
        columns.set(1, PhysicalColumn.of("Order_ID", BasicType.LONG_TYPE, 19L, false, null, null));
        assertOdcsStudy(
                "column-case-difference",
                columns,
                "orders",
                false,
                false,
                "incompatible",
                "MISSING_FIELD");
        assertOdcsStudy(
                "key-order",
                odcsColumns(18, 4),
                "orders",
                false,
                true,
                "incompatible",
                "PRIMARY_KEY_ORDER");
        columns = odcsColumns(18, 4);
        columns.set(2, PhysicalColumn.of("amount", BasicType.STRING_TYPE, 128L, false, null, null));
        assertOdcsStudy(
                "wrong-type-family",
                columns,
                "orders",
                false,
                false,
                "incompatible",
                "TYPE_FAMILY");
        columns = odcsColumns(18, 4);
        columns.set(
                2, PhysicalColumn.of("amount", new DecimalType(18, 4), 16L, 2, false, null, null));
        assertOdcsStudy(
                "conflicting-decimal-metadata",
                columns,
                "orders",
                false,
                false,
                "unsupported",
                "DECIMAL_METADATA_CONFLICT");
        columns = odcsColumns(18, 4);
        columns.set(3, PhysicalColumn.of("note", BasicType.STRING_TYPE, 128L, false, null, null));
        assertOdcsStudy("non-null-note", columns, "orders", false, false, "compatible", "");
    }

    @Test
    void testOdcsStudySourceAndTransformedSnapshotsStaySeparate() throws Exception {
        // Constructed schema pairs only: this does not execute a transform or validate row values.
        assertOdcsStudy(
                "pair-a-source",
                odcsColumns(18, 2),
                "orders",
                false,
                false,
                "incompatible",
                "DECIMAL_INTEGER_CAPACITY",
                "S/source_schema");
        assertOdcsStudy(
                "pair-a-transformed",
                odcsColumns(18, 4),
                "orders",
                false,
                false,
                "compatible",
                "",
                "T/upstream_catalog_table");
        assertOdcsStudy(
                "pair-b-source",
                odcsColumns(16, 2),
                "orders",
                false,
                false,
                "compatible",
                "",
                "S/source_schema");
        assertOdcsStudy(
                "pair-b-transformed",
                odcsColumns(18, 2),
                "orders",
                false,
                false,
                "incompatible",
                "DECIMAL_INTEGER_CAPACITY",
                "T/upstream_catalog_table");
    }

    @Test
    void testOdcsStudyFactoryChangesAreSeparateObservations() throws Exception {
        CatalogTable incoming = odcsTable(odcsColumns(18, 4), "orders", false);
        Map<String, Object> before = odcsSnapshot(incoming);
        Map<String, Object> config = baseConfig();
        config.put("url", "jdbc:mysql://localhost:3306/target_db");
        config.put("driver", "com.mysql.cj.jdbc.Driver");
        config.put("database", "target_db");
        config.put("table", "target_orders");
        config.put(JdbcSinkOptions.TABLE_PREFIX.key(), "p_");
        config.put(JdbcSinkOptions.TABLE_SUFFIX.key(), "_archive");
        config.put("primary_keys", Arrays.asList("order_id", "tenant_id"));
        JdbcSink sink = createJdbcSink(config, incoming);
        CatalogTable writeTable = sink.getWriteCatalogTable().orElseThrow(AssertionError::new);
        Assertions.assertEquals("orders", incoming.getTablePath().getTableName());
        Assertions.assertEquals(
                Arrays.asList("tenant_id", "order_id"),
                incoming.getTableSchema().getPrimaryKey().getColumnNames());
        Assertions.assertEquals(
                "p_target_orders_archive", writeTable.getTablePath().getTableName());
        Assertions.assertEquals("target_db", writeTable.getTablePath().getDatabaseName());
        Assertions.assertEquals(
                Arrays.asList("order_id", "tenant_id"),
                writeTable.getTableSchema().getPrimaryKey().getColumnNames());
        Map<String, Object> result = odcsEnvelope("factory-name-key-overrides");
        result.put("incoming", before);
        result.put("sinkWrite", odcsSnapshot(writeTable));
        result.put("actualTargetOutcome", "unsupported");
        result.put("actualTargetReason", "TARGET_NOT_OBSERVED");
        String json = JsonUtils.toJsonString(result);
        Assertions.assertEquals(
                result.get("contractDigest"),
                JsonUtils.parseObject(json).get("contractDigest").asText());
        Assertions.assertEquals(
                "orders-study", JsonUtils.parseObject(json).get("contractId").asText());
        Assertions.assertEquals(before, odcsSnapshot(incoming));
        System.out.println("ODCS_STUDY " + json);
    }

    private static List<Column> odcsColumns(int precision, int scale) {
        return new ArrayList<>(
                Arrays.asList(
                        PhysicalColumn.of("tenant_id", BasicType.LONG_TYPE, 19L, false, null, null),
                        PhysicalColumn.of("order_id", BasicType.LONG_TYPE, 19L, false, null, null),
                        PhysicalColumn.of(
                                "amount",
                                new DecimalType(precision, scale),
                                (long) precision,
                                scale,
                                false,
                                null,
                                null),
                        PhysicalColumn.of("note", BasicType.STRING_TYPE, 128L, true, null, null)));
    }

    private static CatalogTable odcsTable(List<Column> columns, String name, boolean reverseKey) {
        return CatalogTable.of(
                TableIdentifier.of("study", "source_db", name),
                TableSchema.builder()
                        .columns(columns)
                        .primaryKey(
                                PrimaryKey.of(
                                        "pk_orders",
                                        reverseKey
                                                ? Arrays.asList("order_id", "tenant_id")
                                                : Arrays.asList("tenant_id", "order_id")))
                        .build(),
                new HashMap<>(),
                new ArrayList<>(),
                null);
    }

    private static Map<String, Object> odcsSnapshot(CatalogTable table) {
        Map<String, Object> snapshot = new LinkedHashMap<>();
        snapshot.put("table", table.getTablePath().getFullName());
        snapshot.put(
                "primaryKey",
                new ArrayList<>(table.getTableSchema().getPrimaryKey().getColumnNames()));
        List<Map<String, Object>> fields = new ArrayList<>();
        for (Column column : table.getTableSchema().getColumns()) {
            Map<String, Object> field = new LinkedHashMap<>();
            field.put("name", column.getName());
            field.put("type", column.getDataType().toString());
            field.put("length", column.getColumnLength());
            field.put("scale", column.getScale());
            field.put("nullable", column.isNullable());
            fields.add(field);
        }
        snapshot.put("fields", fields);
        return snapshot;
    }

    private static Map<String, Object> odcsEnvelope(String fixture) throws Exception {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("resultFormatVersion", "study-1");
        result.put("sourceRevision", "39c74ce48b0fb5e0318326b8d57a42c7380c40c8");
        result.put("fixture", fixture);
        result.put("standardVersion", "v3.2.0");
        result.put("contractId", "orders-study");
        result.put("contractVersion", "1.0.0");
        StringBuilder digest = new StringBuilder();
        for (byte value :
                MessageDigest.getInstance("SHA-256")
                        .digest(ODCS_STUDY_CONTRACT.getBytes(StandardCharsets.UTF_8))) {
            digest.append(String.format("%02x", value & 0xff));
        }
        result.put("contractDigest", digest.toString());
        result.put("boundary", "T/upstream_catalog_table");
        result.put("namePolicy", "exact-case");
        result.put("decimalPolicy", "producer-to-consumer-range");
        return result;
    }

    // This evaluator is a test-only specification of the manually mapped scalar fixture,
    // not an ODCS parser, extension point or runtime validator.
    private void assertOdcsStudy(
            String name,
            List<Column> columns,
            String tableName,
            boolean allowExtras,
            boolean reverseKey,
            String expected,
            String expectedReason)
            throws Exception {
        assertOdcsStudy(
                name,
                columns,
                tableName,
                allowExtras,
                reverseKey,
                expected,
                expectedReason,
                "T/upstream_catalog_table");
    }

    private void assertOdcsStudy(
            String name,
            List<Column> columns,
            String tableName,
            boolean allowExtras,
            boolean reverseKey,
            String expected,
            String expectedReason,
            String boundary)
            throws Exception {
        CatalogTable observed = odcsTable(columns, tableName, reverseKey);
        TableSinkFactoryContext context =
                new TableSinkFactoryContext(
                        observed,
                        ReadonlyConfig.fromMap(baseConfig()),
                        getClass().getClassLoader());
        List<Map<String, Object>> findings = new ArrayList<>();
        if (!"orders".equals(context.getCatalogTable().getTablePath().getTableName())) {
            findings.add(odcsFinding("TABLE_NAME", "table", "orders", tableName));
        }
        for (Column required : odcsColumns(18, 4)) {
            Column actual =
                    columns.stream()
                            .filter(c -> c.getName().equals(required.getName()))
                            .findFirst()
                            .orElse(null);
            if (actual == null) {
                findings.add(odcsFinding("MISSING_FIELD", required.getName(), "present", "absent"));
                continue;
            }
            if (!required.isNullable() && actual.isNullable()) {
                findings.add(
                        odcsFinding("NULLABILITY", required.getName(), "not-null", "nullable"));
            }
            if (required.getDataType().getSqlType() != actual.getDataType().getSqlType()) {
                findings.add(
                        odcsFinding(
                                "TYPE_FAMILY",
                                required.getName(),
                                required.getDataType().toString(),
                                actual.getDataType().toString()));
                continue;
            }
            if (actual.getDataType() instanceof DecimalType) {
                DecimalType decimal = (DecimalType) actual.getDataType();
                if (!Long.valueOf(decimal.getPrecision()).equals(actual.getColumnLength())
                        || !Integer.valueOf(decimal.getScale()).equals(actual.getScale())) {
                    findings.add(
                            odcsFinding(
                                    "DECIMAL_METADATA_CONFLICT",
                                    actual.getName(),
                                    decimal.toString(),
                                    actual.getColumnLength() + "," + actual.getScale()));
                    continue;
                }
                if (decimal.getPrecision() - decimal.getScale() > 14) {
                    findings.add(
                            odcsFinding(
                                    "DECIMAL_INTEGER_CAPACITY",
                                    actual.getName(),
                                    14,
                                    decimal.getPrecision() - decimal.getScale()));
                }
                if (decimal.getScale() > 4) {
                    findings.add(
                            odcsFinding("DECIMAL_SCALE", actual.getName(), 4, decimal.getScale()));
                }
            }
        }
        if (!allowExtras) {
            for (Column actual : columns) {
                if (!Arrays.asList("tenant_id", "order_id", "amount", "note")
                        .contains(actual.getName())) {
                    findings.add(odcsFinding("EXTRA_FIELD", actual.getName(), "absent", "present"));
                }
            }
        }
        if (!Arrays.asList("tenant_id", "order_id")
                .equals(
                        context.getCatalogTable()
                                .getTableSchema()
                                .getPrimaryKey()
                                .getColumnNames())) {
            findings.add(
                    odcsFinding(
                            "PRIMARY_KEY_ORDER",
                            "primary-key",
                            Arrays.asList("tenant_id", "order_id"),
                            context.getCatalogTable()
                                    .getTableSchema()
                                    .getPrimaryKey()
                                    .getColumnNames()));
        }
        boolean incompatible =
                findings.stream()
                        .anyMatch(f -> !"DECIMAL_METADATA_CONFLICT".equals(f.get("reason")));
        String outcome =
                incompatible ? "incompatible" : findings.isEmpty() ? "compatible" : "unsupported";
        Assertions.assertEquals(expected, outcome, name);
        if (!expectedReason.isEmpty()) {
            Assertions.assertTrue(
                    findings.stream().anyMatch(f -> expectedReason.equals(f.get("reason"))), name);
        }
        Map<String, Object> result = odcsEnvelope(name);
        result.put("boundary", boundary);
        result.put("evidenceProvenance", "constructed-schema-fixture");
        result.put("allowExtras", allowExtras);
        result.put("observed", odcsSnapshot(context.getCatalogTable()));
        result.put("scalarOutcome", outcome);
        result.put("actualTargetOutcome", "unsupported");
        result.put("actualTargetReason", "TARGET_NOT_OBSERVED");
        result.put("findings", findings);
        result.put(
                "notEvaluated",
                Arrays.asList(
                        "string-character-capacity",
                        "nested-types",
                        "quality",
                        "SLA",
                        "actual-target-metadata"));
        System.out.println("ODCS_STUDY " + JsonUtils.toJsonString(result));
    }

    private static Map<String, Object> odcsFinding(
            String reason, String field, Object expected, Object observed) {
        Map<String, Object> finding = new LinkedHashMap<>();
        finding.put("reason", reason);
        finding.put("field", field);
        finding.put("expected", expected);
        finding.put("observed", observed);
        return finding;
    }

    private final JdbcSinkFactory factory = new JdbcSinkFactory();
    private final OptionRule rule = factory.optionRule();

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule);
    }

    private Map<String, Object> baseConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:oracle:thin:@//localhost:1521/ORCL");
        cfg.put("driver", "oracle.jdbc.OracleDriver");
        cfg.put("schema_save_mode", "CREATE_SCHEMA_WHEN_NOT_EXIST");
        cfg.put("data_save_mode", "APPEND_DATA");
        cfg.put("generate_sink_sql", true);
        cfg.put("database", "ORCL");
        return cfg;
    }

    private Map<String, Object> cockroachConfig() {
        Map<String, Object> cfg = baseConfig();
        cfg.put("url", "jdbc:postgresql://localhost:26257/defaultdb");
        cfg.put("driver", "org.postgresql.Driver");
        cfg.put("dialect", "CockroachDB");
        return cfg;
    }

    private CatalogTable createCatalogTable(boolean withPrimaryKey) {
        TableSchema.Builder schemaBuilder =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.LONG_TYPE, 22, false, null, "id"))
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 128, false, null, "name"));
        if (withPrimaryKey) {
            schemaBuilder.primaryKey(PrimaryKey.of("pk_id", Collections.singletonList("id")));
        }
        return CatalogTable.of(
                TableIdentifier.of("catalog", "ORCL", null, "TEST_TABLE"),
                schemaBuilder.build(),
                new HashMap<>(),
                new ArrayList<>(),
                null,
                "catalog");
    }

    /**
     * A table with a UNIQUE key on {@code name} (column 1) and deliberately no primary key: the
     * factory then takes {@code primary_keys} from the unique key, which is the fallback that the
     * routing tests below need. Adding a primary key would make them test the primary key path.
     */
    private CatalogTable createCatalogTableWithUniqueKeyOnName() {
        TableSchema schema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.LONG_TYPE, 22, false, null, "id"))
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 128, false, null, "name"))
                        .constraintKey(
                                ConstraintKey.of(
                                        ConstraintKey.ConstraintType.UNIQUE_KEY,
                                        "uk_name",
                                        Collections.singletonList(
                                                ConstraintKey.ConstraintKeyColumn.of(
                                                        "name", ConstraintKey.ColumnSortType.ASC))))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("catalog", "ORCL", null, "TEST_TABLE"),
                schema,
                new HashMap<>(),
                new ArrayList<>(),
                null,
                "catalog");
    }

    /**
     * Creates the sink as a job submission does: the option rule validates {@code cfg}, then {@code
     * factory.createSink} derives the effective key, so the sink sees the same {@code primary_keys}
     * and schema as at runtime.
     */
    private JdbcSink createJdbcSink(Map<String, Object> cfg, CatalogTable catalogTable) {
        ReadonlyConfig config = ReadonlyConfig.fromMap(cfg);
        ConfigValidator.of(config).validate(factory.optionRule());
        TableSinkFactoryContext context =
                new TableSinkFactoryContext(catalogTable, config, getClass().getClassLoader());
        return (JdbcSink) factory.createSink(context).createSink();
    }

    /**
     * Simulates the FactoryUtil.createAndPrepareSink entry path: OptionRule validation followed by
     * factory.createSink(context). This covers the real submission-time path end-to-end.
     */
    private TableSink createSinkViaFactoryContext(Map<String, Object> cfg, boolean withPrimaryKey) {
        ReadonlyConfig config = ReadonlyConfig.fromMap(cfg);
        ConfigValidator.of(config).validate(factory.optionRule());
        CatalogTable catalogTable = createCatalogTable(withPrimaryKey);
        TableSinkFactoryContext context =
                new TableSinkFactoryContext(catalogTable, config, getClass().getClassLoader());
        return factory.createSink(context);
    }

    @Test
    void testValidSinkConfig() {
        Assertions.assertDoesNotThrow(() -> validate(baseConfig()));
    }

    @Test
    void testOracleAppendValuesValidConfig() {
        Map<String, Object> cfg = baseConfig();
        cfg.put("oracle_insert_mode", "APPEND_VALUES");
        cfg.put("generate_sink_sql", true);
        cfg.put("auto_commit", true);
        cfg.put("is_exactly_once", false);
        cfg.put("use_copy_statement", false);
        cfg.put("support_upsert_by_insert_only", false);
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testExactlyOnceWithMaxRetriesZero() {
        Map<String, Object> cfg = baseConfig();
        cfg.put("is_exactly_once", true);
        cfg.put("max_retries", 0);
        cfg.put("xa_data_source_class_name", "oracle.jdbc.xa.OracleXADataSource");
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testOracleAppendValuesWithExactlyOnceFails() {
        Map<String, Object> cfg = baseConfig();
        cfg.put("oracle_insert_mode", "APPEND_VALUES");
        cfg.put("generate_sink_sql", true);
        cfg.put("is_exactly_once", true);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testOracleAppendValuesWithCopyStatementFails() {
        Map<String, Object> cfg = baseConfig();
        cfg.put("oracle_insert_mode", "APPEND_VALUES");
        cfg.put("generate_sink_sql", true);
        cfg.put("use_copy_statement", true);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testOracleAppendValuesWithAutoCommitFalseFails() {
        Map<String, Object> cfg = baseConfig();
        cfg.put("oracle_insert_mode", "APPEND_VALUES");
        cfg.put("generate_sink_sql", true);
        cfg.put("auto_commit", false);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testOracleAppendValuesWithCustomQueryFails() {
        Map<String, Object> cfg = baseConfig();
        cfg.put("oracle_insert_mode", "APPEND_VALUES");
        cfg.put("generate_sink_sql", false);
        cfg.remove("database");
        cfg.put("query", "INSERT INTO t VALUES(?,?)");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testOracleAppendValuesWithInsertOnlyUpsertFails() {
        Map<String, Object> cfg = baseConfig();
        cfg.put("oracle_insert_mode", "APPEND_VALUES");
        cfg.put("generate_sink_sql", true);
        cfg.put("support_upsert_by_insert_only", true);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testExactlyOnceWithMaxRetriesNonZeroFails() {
        Map<String, Object> cfg = baseConfig();
        cfg.put("is_exactly_once", true);
        cfg.put("max_retries", 3);
        cfg.put("xa_data_source_class_name", "oracle.jdbc.xa.OracleXADataSource");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testMissingUrlFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("driver", "oracle.jdbc.OracleDriver");
        cfg.put("schema_save_mode", "CREATE_SCHEMA_WHEN_NOT_EXIST");
        cfg.put("data_save_mode", "APPEND_DATA");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testMissingDriverFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("url", "jdbc:oracle:thin:@//localhost:1521/ORCL");
        cfg.put("schema_save_mode", "CREATE_SCHEMA_WHEN_NOT_EXIST");
        cfg.put("data_save_mode", "APPEND_DATA");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    /**
     * The strict unknown-key check used by {@code --check} and {@code --dry-run} only accepts keys
     * declared in the option rule, so every option the sink reads must be declared there.
     */
    @Test
    void testDocumentedSinkOptionsAreNotUnknownKeys() {
        Map<String, Object> cfg = baseConfig();
        cfg.put("table", "TEST_TABLE");
        cfg.put("schema", "ST");
        cfg.put("properties", Collections.singletonMap("oracle.jdbc.timezoneAsRegion", "false"));
        cfg.put("connect_timeout_ms", 30000);
        cfg.put("socket_timeout_ms", 60000);
        cfg.put("decimal_type_narrowing", true);
        cfg.put("int_type_narrowing", true);
        cfg.put("handle_blob_as_string", false);
        cfg.put("use_kerberos", true);
        cfg.put("kerberos_principal", "seatunnel@EXAMPLE.COM");
        cfg.put("kerberos_keytab_path", "/etc/seatunnel.keytab");
        cfg.put("krb5_path", "/etc/krb5.conf");
        cfg.put("access_key_id", "ak");
        cfg.put("secret_access_key", "sk");
        cfg.put("region", "us-east-1");

        Assertions.assertDoesNotThrow(
                () ->
                        ConfigValidator.validateUnknownKeys(
                                ReadonlyConfig.fromMap(cfg), rule, "Jdbc"));
    }

    @Test
    void testUndeclaredSinkOptionIsUnknownKey() {
        Map<String, Object> cfg = baseConfig();
        cfg.put("tabel", "TEST_TABLE");

        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () ->
                                ConfigValidator.validateUnknownKeys(
                                        ReadonlyConfig.fromMap(cfg), rule, "Jdbc"));
        Assertions.assertTrue(exception.getMessage().contains("[tabel]"));
    }

    // ---- Entry-level regression tests through factory-context path ----

    @Test
    void testFactoryContextPathValidConfig() {
        Map<String, Object> cfg = baseConfig();
        Assertions.assertDoesNotThrow(() -> createSinkViaFactoryContext(cfg, false));
    }

    @Test
    void testFactoryContextPathAppendValuesValidConfig() {
        Map<String, Object> cfg = baseConfig();
        cfg.put("oracle_insert_mode", "APPEND_VALUES");
        cfg.put("auto_commit", true);
        cfg.put("is_exactly_once", false);
        cfg.put("use_copy_statement", false);
        cfg.put("support_upsert_by_insert_only", false);
        Assertions.assertDoesNotThrow(() -> createSinkViaFactoryContext(cfg, false));
    }

    @Test
    void testFactoryContextPathAppendValuesWithExactlyOnceFails() {
        Map<String, Object> cfg = baseConfig();
        cfg.put("oracle_insert_mode", "APPEND_VALUES");
        cfg.put("is_exactly_once", true);
        Assertions.assertThrows(
                OptionValidationException.class, () -> createSinkViaFactoryContext(cfg, false));
    }

    /**
     * Verifies the CatalogTable-derived primary keys + APPEND_VALUES scenario.
     *
     * <p>When the upstream CatalogTable carries a primary key but the user does not set {@code
     * primary_keys} in config, {@link JdbcSinkFactory#createSink} auto-populates primary_keys from
     * the catalog schema. The OptionRule validation passes because the primary_keys conflict cannot
     * be detected at config time (it depends on the catalog schema). The runtime guard in {@code
     * JdbcOutputFormatBuilder.validateOracleInsertMode} catches this case and rejects APPEND_VALUES
     * with non-empty primary keys.
     */
    @Test
    void testFactoryContextAppendValuesWithCatalogDerivedPrimaryKeys() {
        Map<String, Object> cfg = baseConfig();
        cfg.put("oracle_insert_mode", "APPEND_VALUES");
        cfg.put("auto_commit", true);
        cfg.put("is_exactly_once", false);
        cfg.put("use_copy_statement", false);
        cfg.put("support_upsert_by_insert_only", false);

        Assertions.assertDoesNotThrow(
                () -> createSinkViaFactoryContext(cfg, true),
                "Factory-level validation and createSink should succeed even when "
                        + "CatalogTable has primary keys; the PK + APPEND_VALUES conflict "
                        + "is guarded at runtime by JdbcOutputFormatBuilder.validateOracleInsertMode");
    }

    @Test
    void cockroachDoesNotAdvertiseSchemaEvolutionWhilePostgresStillDoes() {
        Map<String, Object> cfg = cockroachConfig();
        JdbcSink cockroach = (JdbcSink) createSinkViaFactoryContext(cfg, true).createSink();
        Assertions.assertTrue(cockroach.supports().isEmpty());

        cfg.put("dialect", "Postgres");
        JdbcSink postgres = (JdbcSink) createSinkViaFactoryContext(cfg, true).createSink();
        Assertions.assertTrue(postgres.supports().contains(SchemaChangeType.ADD_COLUMN));
    }

    @Test
    void cockroachRejectsSaveModesThatWouldOtherwiseBeSkipped() {
        Map<String, Object> cfg = cockroachConfig();
        cfg.put("schema_save_mode", "IGNORE");
        Assertions.assertDoesNotThrow(() -> createSinkViaFactoryContext(cfg, true));
        cfg.put("custom_sql", "DELETE FROM public.target");

        for (String dataMode :
                new String[] {"DROP_DATA", "CUSTOM_PROCESSING", "ERROR_WHEN_DATA_EXISTS"}) {
            cfg.put("data_save_mode", dataMode);
            Assertions.assertThrows(
                    JdbcConnectorException.class, () -> createSinkViaFactoryContext(cfg, true));
        }
        cfg.put("data_save_mode", "APPEND_DATA");
        for (String schemaMode : new String[] {"RECREATE_SCHEMA", "ERROR_WHEN_SCHEMA_NOT_EXIST"}) {
            cfg.put("schema_save_mode", schemaMode);
            Assertions.assertThrows(
                    JdbcConnectorException.class, () -> createSinkViaFactoryContext(cfg, true));
        }
    }

    /**
     * A table with only a UNIQUE key gets {@code primary_keys} from it, so the sink upserts by that
     * column. Its writers must route by the same column, or the changes of one row could reach
     * different writers when {@code multi_table_sink_replica} is above 1.
     */
    @Test
    void testUniqueKeyOnlyTableRoutesByItsUniqueColumn() {
        JdbcSink sink = createJdbcSink(baseConfig(), createCatalogTableWithUniqueKeyOnName());

        Assertions.assertEquals(1, sink.routingKeyIndex().get());
    }

    @Test
    void testRoutingKeyFollowsConfiguredPrimaryKeysAndIsEmptyWithoutAnyKey() {
        Map<String, Object> cfg = baseConfig();
        cfg.put("primary_keys", Arrays.asList("name"));
        Assertions.assertEquals(
                1, createJdbcSink(cfg, createCatalogTable(false)).routingKeyIndex().get());

        Assertions.assertFalse(
                createJdbcSink(baseConfig(), createCatalogTable(false))
                        .routingKeyIndex()
                        .isPresent());
        Assertions.assertEquals(
                0, createJdbcSink(baseConfig(), createCatalogTable(true)).routingKeyIndex().get());
    }

    /**
     * Configured {@code primary_keys} replace the primary key of the table for upserts, so the
     * writers route by them too, not by the primary key of the table.
     */
    @Test
    void testConfiguredPrimaryKeysRouteInsteadOfThePrimaryKeyOfTheTable() {
        Map<String, Object> cfg = baseConfig();
        cfg.put("primary_keys", Arrays.asList("name"));

        Assertions.assertEquals(
                1, createJdbcSink(cfg, createCatalogTable(true)).routingKeyIndex().get());
    }
}
