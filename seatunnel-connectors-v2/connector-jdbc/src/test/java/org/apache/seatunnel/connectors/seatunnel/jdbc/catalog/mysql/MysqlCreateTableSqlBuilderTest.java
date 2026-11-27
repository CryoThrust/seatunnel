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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MySqlTypeConverter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MysqlCreateTableSqlBuilderTest {

    /** Executes only the SQL builder, not CREATE TABLE or a database metadata read. */
    @Test
    public void testOdcsStudySeparatesTypedDecimalFromPlannedNativeSql() {
        assertOdcsPlannedType("typed-decimal", 18, 4, null, null, "DECIMAL(18,4)");
        assertOdcsPlannedType("explicit-sink-type", 18, 4, "DECIMAL(18,2)", null, "DECIMAL(18,2)");
        assertOdcsPlannedType(
                "preserved-source-type", 18, 4, null, "DECIMAL(16,2)", "DECIMAL(16,2)");
        assertOdcsPlannedType("converter-normalization", 70, 35, null, null, "DECIMAL(65,30)");
    }

    private void assertOdcsPlannedType(
            String fixture,
            int precision,
            int scale,
            String sinkType,
            String sourceType,
            String expected) {
        Column amount =
                PhysicalColumn.of(
                        "amount",
                        new DecimalType(precision, scale),
                        (long) precision,
                        scale,
                        false,
                        null,
                        null,
                        sinkType,
                        sourceType);
        TablePath path = TablePath.of("target_db", "orders");
        CatalogTable table =
                CatalogTable.of(
                        TableIdentifier.of("mysql", "target_db", "orders"),
                        TableSchema.builder().column(amount).build(),
                        new HashMap<>(),
                        Collections.emptyList(),
                        null);
        String sql =
                MysqlCreateTableSqlBuilder.builder(
                                path, table, MySqlTypeConverter.DEFAULT_INSTANCE, false)
                        .build(DatabaseIdentifier.MYSQL);
        Assertions.assertTrue(sql.contains("`amount` " + expected + " NOT NULL"), sql);
        Map<String, Object> observation = new LinkedHashMap<>();
        observation.put("resultFormatVersion", "study-1");
        observation.put("sourceRevision", "39c74ce48b0fb5e0318326b8d57a42c7380c40c8");
        observation.put("fixture", fixture);
        observation.put("boundary", "planned-ddl-only");
        observation.put("contractEvaluation", "not-performed");
        observation.put("typedDecimal", amount.getDataType().toString());
        observation.put("sourceType", sourceType);
        observation.put("sinkType", sinkType);
        observation.put("plannedNativeType", expected);
        observation.put("sql", sql);
        observation.put("actualTargetOutcome", "unsupported");
        observation.put("actualTargetReason", "TARGET_NOT_OBSERVED");
        System.out.println("ODCS_STUDY " + JsonUtils.toJsonString(observation));
    }

    @Test
    public void testOdcsStudyNullableKeyDependsOnIndexCreation() {
        CatalogTable table =
                CatalogTable.of(
                        TableIdentifier.of("mysql", "target_db", "orders"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "order_id",
                                                BasicType.LONG_TYPE,
                                                19L,
                                                true,
                                                null,
                                                null))
                                .primaryKey(
                                        PrimaryKey.of(
                                                "pk_orders", Collections.singletonList("order_id")))
                                .build(),
                        new HashMap<>(),
                        Collections.emptyList(),
                        null);
        for (boolean createIndex : Arrays.asList(false, true)) {
            String sql =
                    MysqlCreateTableSqlBuilder.builder(
                                    table.getTablePath(),
                                    table,
                                    MySqlTypeConverter.DEFAULT_INSTANCE,
                                    createIndex)
                            .build(DatabaseIdentifier.MYSQL);
            Assertions.assertTrue(
                    sql.contains("`order_id` BIGINT " + (createIndex ? "NOT NULL" : "NULL")), sql);
            Assertions.assertEquals(createIndex, sql.contains("PRIMARY KEY (`order_id`)"));
            Assertions.assertTrue(table.getTableSchema().getColumns().get(0).isNullable());
            Map<String, Object> observation = new LinkedHashMap<>();
            observation.put("resultFormatVersion", "study-1");
            observation.put("sourceRevision", "39c74ce48b0fb5e0318326b8d57a42c7380c40c8");
            observation.put("fixture", "nullable-key-create-index-" + createIndex);
            observation.put("boundary", "planned-ddl-only");
            observation.put("contractEvaluation", "not-performed");
            observation.put("typedNullable", true);
            observation.put("plannedNullable", !createIndex);
            observation.put("sql", sql);
            observation.put("actualTargetOutcome", "unsupported");
            observation.put("actualTargetReason", "TARGET_NOT_OBSERVED");
            System.out.println("ODCS_STUDY " + JsonUtils.toJsonString(observation));
        }
    }

    private static final PrintStream CONSOLE = System.out;

    @Test
    public void testBuild() {
        // todo
        String dataBaseName = "test_database";
        String tableName = "test_table";
        TablePath tablePath = TablePath.of(dataBaseName, tableName);
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.LONG_TYPE, 22, false, null, "id"))
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 128, false, null, "name"))
                        .column(
                                PhysicalColumn.of(
                                        "age", BasicType.INT_TYPE, (Long) null, true, null, "age"))
                        .column(
                                PhysicalColumn.of(
                                        "blob_v",
                                        PrimitiveByteArrayType.INSTANCE,
                                        Long.MAX_VALUE,
                                        true,
                                        null,
                                        "blob_v"))
                        .column(
                                PhysicalColumn.of(
                                        "createTime",
                                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                        3,
                                        true,
                                        null,
                                        "createTime"))
                        .column(
                                PhysicalColumn.of(
                                        "lastUpdateTime",
                                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                        3,
                                        true,
                                        null,
                                        "lastUpdateTime"))
                        .primaryKey(PrimaryKey.of("id", Lists.newArrayList("id")))
                        .constraintKey(
                                Arrays.asList(
                                        ConstraintKey.of(
                                                ConstraintKey.ConstraintType.INDEX_KEY,
                                                "name",
                                                Lists.newArrayList(
                                                        ConstraintKey.ConstraintKeyColumn.of(
                                                                "name", null))),
                                        ConstraintKey.of(
                                                ConstraintKey.ConstraintType.INDEX_KEY,
                                                "blob_v",
                                                Lists.newArrayList(
                                                        ConstraintKey.ConstraintKeyColumn.of(
                                                                "blob_v", null)))))
                        .build();
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test_catalog", dataBaseName, tableName),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        "User table");

        String createTableSql =
                MysqlCreateTableSqlBuilder.builder(
                                tablePath, catalogTable, MySqlTypeConverter.DEFAULT_INSTANCE, true)
                        .build(DatabaseIdentifier.MYSQL);
        // create table sql is change; The old unit tests are no longer applicable
        String expect =
                "CREATE TABLE `test_table` (\n"
                        + "\t`id` BIGINT NOT NULL COMMENT 'id', \n"
                        + "\t`name` VARCHAR(128) NOT NULL COMMENT 'name', \n"
                        + "\t`age` INT NULL COMMENT 'age', \n"
                        + "\t`blob_v` LONGBLOB NULL COMMENT 'blob_v', \n"
                        + "\t`createTime` DATETIME NULL COMMENT 'createTime', \n"
                        + "\t`lastUpdateTime` DATETIME NULL COMMENT 'lastUpdateTime', \n"
                        + "\tPRIMARY KEY (`id`), \n"
                        + "\tKEY `name` (`name`), \n"
                        + "\tKEY `blob_v` (`blob_v`(255))\n"
                        + ") COMMENT = 'User table';";
        CONSOLE.println(expect);
        Assertions.assertEquals(expect, createTableSql);

        // skip index
        String createTableSqlSkipIndex =
                MysqlCreateTableSqlBuilder.builder(
                                tablePath, catalogTable, MySqlTypeConverter.DEFAULT_INSTANCE, false)
                        .build(DatabaseIdentifier.MYSQL);
        String expectSkipIndex =
                "CREATE TABLE `test_table` (\n"
                        + "\t`id` BIGINT NOT NULL COMMENT 'id', \n"
                        + "\t`name` VARCHAR(128) NOT NULL COMMENT 'name', \n"
                        + "\t`age` INT NULL COMMENT 'age', \n"
                        + "\t`blob_v` LONGBLOB NULL COMMENT 'blob_v', \n"
                        + "\t`createTime` DATETIME NULL COMMENT 'createTime', \n"
                        + "\t`lastUpdateTime` DATETIME NULL COMMENT 'lastUpdateTime'\n"
                        + ") COMMENT = 'User table';";
        CONSOLE.println(expectSkipIndex);
        Assertions.assertEquals(expectSkipIndex, createTableSqlSkipIndex);
    }

    @Test
    public void testBuildCreateTableSqlWithTableOptions() {
        TablePath tablePath = TablePath.of("test_db", "test_table");
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.LONG_TYPE, 0, false, null, "id"))
                        .primaryKey(PrimaryKey.of("id", Lists.newArrayList("id")))
                        .build();
        Map<String, String> options = new HashMap<>();
        options.put(MySqlCatalog.TABLE_OPTION_ENGINE, "InnoDB");
        options.put(MySqlCatalog.TABLE_OPTION_CHARSET, "utf8mb4");
        options.put(MySqlCatalog.TABLE_OPTION_COLLATE, "utf8mb4_unicode_ci");
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test_catalog", "test_db", "test_table"),
                        tableSchema,
                        options,
                        Collections.emptyList(),
                        "table with options");

        String createTableSql =
                MysqlCreateTableSqlBuilder.builder(
                                tablePath, catalogTable, MySqlTypeConverter.DEFAULT_INSTANCE, true)
                        .build(DatabaseIdentifier.MYSQL);

        Assertions.assertTrue(createTableSql.contains("ENGINE = InnoDB"));
        Assertions.assertTrue(createTableSql.contains("DEFAULT CHARSET = utf8mb4"));
        Assertions.assertTrue(createTableSql.contains("COLLATE = utf8mb4_unicode_ci"));
    }

    @Test
    public void testNullablePrimaryKeyColumnIsNotNullWhenCreateIndex() {
        TablePath tablePath = TablePath.of("test_db", "test_table");
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.INT_TYPE, 0, true, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "description", BasicType.STRING_TYPE, 32, true, null, null))
                        .primaryKey(PrimaryKey.of("id", Lists.newArrayList("id")))
                        .build();
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test_catalog", "test_db", "test_table"),
                        tableSchema,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "test table");

        String createTableSql =
                MysqlCreateTableSqlBuilder.builder(
                                tablePath, catalogTable, MySqlTypeConverter.DEFAULT_INSTANCE, true)
                        .build(DatabaseIdentifier.MYSQL);

        Assertions.assertTrue(createTableSql.contains("`id` INT NOT NULL"));
        Assertions.assertTrue(createTableSql.contains("`description` VARCHAR(32) NULL"));
        Assertions.assertTrue(createTableSql.contains("PRIMARY KEY (`id`)"));

        String createTableSqlWithoutIndex =
                MysqlCreateTableSqlBuilder.builder(
                                tablePath, catalogTable, MySqlTypeConverter.DEFAULT_INSTANCE, false)
                        .build(DatabaseIdentifier.MYSQL);

        Assertions.assertTrue(createTableSqlWithoutIndex.contains("`id` INT NULL"));
        Assertions.assertFalse(createTableSqlWithoutIndex.contains("PRIMARY KEY"));
    }

    @Test
    public void testColumnSinkType() {
        MysqlCreateTableSqlBuilder sqlBuilder = mock(MysqlCreateTableSqlBuilder.class);

        Column column = mock(Column.class);
        when(column.getSinkType()).thenReturn("VARCHAR(10)");
        when(column.getDataType()).thenReturn((SeaTunnelDataType) BasicType.INT_TYPE);
        when(column.getName()).thenReturn("col1");
        when(sqlBuilder.buildColumnIdentifySql(column, null, new HashMap<>())).thenCallRealMethod();

        String result = sqlBuilder.buildColumnIdentifySql(column, null, new HashMap<>());

        Assertions.assertEquals("`col1` VARCHAR(10) NOT NULL", result);
    }

    @Test
    public void testBuildEscapeTableComment() {
        String dataBaseName = "test_database";
        String tableName = "test_table";
        TablePath tablePath = TablePath.of(dataBaseName, tableName);
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.LONG_TYPE, 22, false, null, null))
                        .build();
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test_catalog", dataBaseName, tableName),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        "table's \\\\ comment");

        String createTableSql =
                MysqlCreateTableSqlBuilder.builder(
                                tablePath, catalogTable, MySqlTypeConverter.DEFAULT_INSTANCE, false)
                        .build(DatabaseIdentifier.MYSQL);

        Assertions.assertTrue(createTableSql.endsWith("COMMENT = 'table''s \\\\\\\\ comment';"));
    }
}
