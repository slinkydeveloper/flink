/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.hbase1;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.hbase.options.HBaseLookupOptions;
import org.apache.flink.connector.hbase.options.HBaseWriteOptions;
import org.apache.flink.connector.hbase.source.HBaseRowDataLookupFunction;
import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.connector.hbase1.sink.HBaseDynamicTableSink;
import org.apache.flink.connector.hbase1.source.HBaseDynamicTableSource;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.LookupRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.ExceptionUtils;

import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.hbase.HConstants;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Unit test for {@link HBase1DynamicTableFactory}. */
public class HBaseDynamicTableFactoryTest {

    private static final String FAMILY1 = "f1";
    private static final String FAMILY2 = "f2";
    private static final String FAMILY3 = "f3";
    private static final String FAMILY4 = "f4";
    private static final String COL1 = "c1";
    private static final String COL2 = "c2";
    private static final String COL3 = "c3";
    private static final String COL4 = "c4";
    private static final String ROWKEY = "rowkey";

    @Rule public final ExpectedException thrown = ExpectedException.none();

    @SuppressWarnings("rawtypes")
    @Test
    public void testTableSourceFactory() {
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical(FAMILY1, ROW(FIELD(COL1, INT()))),
                        Column.physical(FAMILY2, ROW(FIELD(COL1, INT()), FIELD(COL2, BIGINT()))),
                        Column.physical(ROWKEY, BIGINT()),
                        Column.physical(
                                FAMILY3,
                                ROW(
                                        FIELD(COL1, DOUBLE()),
                                        FIELD(COL2, BOOLEAN()),
                                        FIELD(COL3, STRING()))),
                        Column.physical(
                                FAMILY4,
                                ROW(
                                        FIELD(COL1, DECIMAL(10, 3)),
                                        FIELD(COL2, TIMESTAMP(3)),
                                        FIELD(COL3, DATE()),
                                        FIELD(COL4, TIME()))));

        DynamicTableSource source = createTableSource(schema, getAllOptions());
        assertThat(source).isInstanceOf(HBaseDynamicTableSource.class);
        HBaseDynamicTableSource hbaseSource = (HBaseDynamicTableSource) source;

        int[][] lookupKey = {{2}};
        LookupTableSource.LookupRuntimeProvider lookupProvider =
                hbaseSource.getLookupRuntimeProvider(new LookupRuntimeProviderContext(lookupKey));
        assertThat(lookupProvider).isInstanceOf(TableFunctionProvider.class);

        TableFunction tableFunction =
                ((TableFunctionProvider) lookupProvider).createTableFunction();
        assertThat(tableFunction).isInstanceOf(HBaseRowDataLookupFunction.class);
        assertThat(((HBaseRowDataLookupFunction) tableFunction).getHTableName())
                .isEqualTo("testHBastTable");

        HBaseTableSchema hbaseSchema = hbaseSource.getHBaseTableSchema();
        assertThat(hbaseSchema.getRowKeyIndex()).isEqualTo(2);
        assertThat(hbaseSchema.getRowKeyTypeInfo()).isEqualTo(Optional.of(Types.LONG));

        assertThat(hbaseSchema.getFamilyNames()).isEqualTo(new String[] {"f1", "f2", "f3", "f4"});
        assertThat(hbaseSchema.getQualifierNames("f1")).isEqualTo(new String[] {"c1"});
        assertThat(hbaseSchema.getQualifierNames("f2")).isEqualTo(new String[] {"c1", "c2"});
        assertThat(hbaseSchema.getQualifierNames("f3")).isEqualTo(new String[] {"c1", "c2", "c3"});
        assertThat(hbaseSchema.getQualifierNames("f4"))
                .isEqualTo(new String[] {"c1", "c2", "c3", "c4"});

        assertThat(hbaseSchema.getQualifierDataTypes("f1")).isEqualTo(new DataType[] {INT()});
        assertThat(hbaseSchema.getQualifierDataTypes("f2"))
                .isEqualTo(new DataType[] {INT(), BIGINT()});
        assertThat(hbaseSchema.getQualifierDataTypes("f3"))
                .isEqualTo(new DataType[] {DOUBLE(), BOOLEAN(), STRING()});
        assertThat(hbaseSchema.getQualifierDataTypes("f4"))
                .isEqualTo(new DataType[] {DECIMAL(10, 3), TIMESTAMP(3), DATE(), TIME()});
    }

    @Test
    public void testTableSinkFactory() {
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical(ROWKEY, STRING()),
                        Column.physical(FAMILY1, ROW(FIELD(COL1, DOUBLE()), FIELD(COL2, INT()))),
                        Column.physical(FAMILY2, ROW(FIELD(COL1, INT()), FIELD(COL3, BIGINT()))),
                        Column.physical(
                                FAMILY3, ROW(FIELD(COL2, BOOLEAN()), FIELD(COL3, STRING()))),
                        Column.physical(
                                FAMILY4,
                                ROW(
                                        FIELD(COL1, DECIMAL(10, 3)),
                                        FIELD(COL2, TIMESTAMP(3)),
                                        FIELD(COL3, DATE()),
                                        FIELD(COL4, TIME()))));

        DynamicTableSink sink = createTableSink(schema, getAllOptions());
        assertThat(sink).isInstanceOf(HBaseDynamicTableSink.class);
        HBaseDynamicTableSink hbaseSink = (HBaseDynamicTableSink) sink;

        HBaseTableSchema hbaseSchema = hbaseSink.getHBaseTableSchema();
        assertThat(hbaseSchema.getRowKeyIndex()).isEqualTo(0);
        assertThat(hbaseSchema.getRowKeyDataType()).isEqualTo(Optional.of(STRING()));

        assertThat(hbaseSchema.getFamilyNames()).isEqualTo(new String[] {"f1", "f2", "f3", "f4"});
        assertThat(hbaseSchema.getQualifierNames("f1")).isEqualTo(new String[] {"c1", "c2"});
        assertThat(hbaseSchema.getQualifierNames("f2")).isEqualTo(new String[] {"c1", "c3"});
        assertThat(hbaseSchema.getQualifierNames("f3")).isEqualTo(new String[] {"c2", "c3"});
        assertThat(hbaseSchema.getQualifierNames("f4"))
                .isEqualTo(new String[] {"c1", "c2", "c3", "c4"});

        assertThat(hbaseSchema.getQualifierDataTypes("f1"))
                .isEqualTo(new DataType[] {DOUBLE(), INT()});
        assertThat(hbaseSchema.getQualifierDataTypes("f2"))
                .isEqualTo(new DataType[] {INT(), BIGINT()});
        assertThat(hbaseSchema.getQualifierDataTypes("f3"))
                .isEqualTo(new DataType[] {BOOLEAN(), STRING()});
        assertThat(hbaseSchema.getQualifierDataTypes("f4"))
                .isEqualTo(new DataType[] {DECIMAL(10, 3), TIMESTAMP(3), DATE(), TIME()});

        // verify hadoop Configuration
        org.apache.hadoop.conf.Configuration expectedConfiguration =
                HBaseConfigurationUtil.getHBaseConfiguration();
        expectedConfiguration.set(HConstants.ZOOKEEPER_QUORUM, "localhost:2181");
        expectedConfiguration.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/flink");
        expectedConfiguration.set("hbase.security.authentication", "kerberos");

        org.apache.hadoop.conf.Configuration actualConfiguration = hbaseSink.getConfiguration();

        assertThat(IteratorUtils.toList(actualConfiguration.iterator()))
                .isEqualTo(IteratorUtils.toList(expectedConfiguration.iterator()));

        // verify tableName
        assertThat(hbaseSink.getTableName()).isEqualTo("testHBastTable");

        HBaseWriteOptions expectedWriteOptions =
                HBaseWriteOptions.builder()
                        .setBufferFlushMaxRows(1000)
                        .setBufferFlushIntervalMillis(1000)
                        .setBufferFlushMaxSizeInBytes(2 * 1024 * 1024)
                        .build();
        HBaseWriteOptions actualWriteOptions = hbaseSink.getWriteOptions();
        assertThat(actualWriteOptions).isEqualTo(expectedWriteOptions);
    }

    @Test
    public void testBufferFlushOptions() {
        Map<String, String> options = getAllOptions();
        options.put("sink.buffer-flush.max-size", "10mb");
        options.put("sink.buffer-flush.max-rows", "100");
        options.put("sink.buffer-flush.interval", "10s");

        ResolvedSchema schema = ResolvedSchema.of(Column.physical(ROWKEY, STRING()));

        DynamicTableSink sink = createTableSink(schema, options);
        HBaseWriteOptions expected =
                HBaseWriteOptions.builder()
                        .setBufferFlushMaxRows(100)
                        .setBufferFlushIntervalMillis(10 * 1000)
                        .setBufferFlushMaxSizeInBytes(10 * 1024 * 1024)
                        .build();
        HBaseWriteOptions actual = ((HBaseDynamicTableSink) sink).getWriteOptions();
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testParallelismOptions() {
        Map<String, String> options = getAllOptions();
        options.put("sink.parallelism", "2");

        ResolvedSchema schema = ResolvedSchema.of(Column.physical(ROWKEY, STRING()));

        DynamicTableSink sink = createTableSink(schema, options);
        assertThat(sink).isInstanceOf(HBaseDynamicTableSink.class);
        HBaseDynamicTableSink hbaseSink = (HBaseDynamicTableSink) sink;
        SinkFunctionProvider provider =
                (SinkFunctionProvider)
                        hbaseSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        assertThat((long) provider.getParallelism().get()).isEqualTo(2);
    }

    @Test
    public void testLookupOptions() {
        Map<String, String> options = getAllOptions();
        options.put("lookup.cache.max-rows", "1000");
        options.put("lookup.cache.ttl", "10s");
        options.put("lookup.max-retries", "10");
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical(ROWKEY, STRING()),
                        Column.physical(FAMILY1, ROW(FIELD(COL1, DOUBLE()), FIELD(COL2, INT()))));
        DynamicTableSource source = createTableSource(schema, options);
        HBaseLookupOptions actual = ((HBaseDynamicTableSource) source).getLookupOptions();
        HBaseLookupOptions expected =
                HBaseLookupOptions.builder()
                        .setCacheMaxSize(1000)
                        .setCacheExpireMs(10_000)
                        .setMaxRetryTimes(10)
                        .build();
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testDisabledBufferFlushOptions() {
        Map<String, String> options = getAllOptions();
        options.put("sink.buffer-flush.max-size", "0");
        options.put("sink.buffer-flush.max-rows", "0");
        options.put("sink.buffer-flush.interval", "0");

        ResolvedSchema schema = ResolvedSchema.of(Column.physical(ROWKEY, STRING()));

        DynamicTableSink sink = createTableSink(schema, options);
        HBaseWriteOptions expected =
                HBaseWriteOptions.builder()
                        .setBufferFlushMaxRows(0)
                        .setBufferFlushIntervalMillis(0)
                        .setBufferFlushMaxSizeInBytes(0)
                        .build();
        HBaseWriteOptions actual = ((HBaseDynamicTableSink) sink).getWriteOptions();
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testUnknownOption() {
        Map<String, String> options = getAllOptions();
        options.put("sink.unknown.key", "unknown-value");
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical(ROWKEY, STRING()),
                        Column.physical(FAMILY1, ROW(FIELD(COL1, DOUBLE()), FIELD(COL2, INT()))));

        try {
            createTableSource(schema, options);
            fail("Should fail");
        } catch (Exception e) {
            assertThat(
                            ExceptionUtils.findThrowableWithMessage(
                                            e, "Unsupported options:\n\nsink.unknown.key")
                                    .isPresent())
                    .isTrue();
        }

        try {
            createTableSink(schema, options);
            fail("Should fail");
        } catch (Exception e) {
            assertThat(
                            ExceptionUtils.findThrowableWithMessage(
                                            e, "Unsupported options:\n\nsink.unknown.key")
                                    .isPresent())
                    .isTrue();
        }
    }

    @Test
    public void testTypeWithUnsupportedPrecision() {
        Map<String, String> options = getAllOptions();
        // test unsupported timestamp precision
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical(ROWKEY, STRING()),
                        Column.physical(
                                FAMILY1, ROW(FIELD(COL1, TIMESTAMP(6)), FIELD(COL2, INT()))));
        try {
            createTableSource(schema, options);
            fail("Should fail");
        } catch (Exception e) {
            assertThat(
                            ExceptionUtils.findThrowableWithMessage(
                                            e,
                                            "The precision 6 of TIMESTAMP type is out of the range [0, 3]"
                                                    + " supported by HBase connector")
                                    .isPresent())
                    .isTrue();
        }

        try {
            createTableSink(schema, options);
            fail("Should fail");
        } catch (Exception e) {
            assertThat(
                            ExceptionUtils.findThrowableWithMessage(
                                            e,
                                            "The precision 6 of TIMESTAMP type is out of the range [0, 3]"
                                                    + " supported by HBase connector")
                                    .isPresent())
                    .isTrue();
        }
        // test unsupported time precision
        schema =
                ResolvedSchema.of(
                        Column.physical(ROWKEY, STRING()),
                        Column.physical(FAMILY1, ROW(FIELD(COL1, TIME(6)), FIELD(COL2, INT()))));

        try {
            createTableSource(schema, options);
            fail("Should fail");
        } catch (Exception e) {
            assertThat(
                            ExceptionUtils.findThrowableWithMessage(
                                            e,
                                            "The precision 6 of TIME type is out of the range [0, 3]"
                                                    + " supported by HBase connector")
                                    .isPresent())
                    .isTrue();
        }

        try {
            createTableSink(schema, options);
            fail("Should fail");
        } catch (Exception e) {
            assertThat(
                            ExceptionUtils.findThrowableWithMessage(
                                            e,
                                            "The precision 6 of TIME type is out of the range [0, 3]"
                                                    + " supported by HBase connector")
                                    .isPresent())
                    .isTrue();
        }
    }

    private Map<String, String> getAllOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "hbase-1.4");
        options.put("table-name", "testHBastTable");
        options.put("zookeeper.quorum", "localhost:2181");
        options.put("zookeeper.znode.parent", "/flink");
        options.put("properties.hbase.security.authentication", "kerberos");
        return options;
    }
}
