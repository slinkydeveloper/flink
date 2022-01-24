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

package org.apache.flink.table.test.delegatingsink;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.sink.SinkProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.test.actions.MockActions;
import org.apache.flink.table.test.actions.RowDataConsumer;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * This sink factory allows to inject an action before executing the sink code. This doesn't work
 * with {@link StreamTableEnvironment#toDataStream(Table, Class)} sinks.
 *
 * <p>The options of the delegate can be included directly in the same map, without any prefix:
 * <code>
 *     identifier = 'delegating'
 *     action = 'org.apache.flink.table.somepackage.MyActionClass'
 *     delegate = 'filesystem'
 *     path = "file-path'
 * </code>
 */
public class DelegatingDynamicTableSinkFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "delegating";

    public static final ConfigOption<String> DELEGATE =
            ConfigOptions.key("delegate")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Identifier of the factory to delegate to.");
    public static final ConfigOption<String> ACTION =
            ConfigOptions.key("consumer")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Identifier of the action implementing "
                                    + RowDataConsumer.class.getCanonicalName()
                                    + ", registered with MockActions#register");

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        ReadableConfig tableConfig = Configuration.fromMap(context.getCatalogTable().getOptions());
        String delegateFactoryIdentifier = tableConfig.get(DELEGATE);
        String actionId = tableConfig.get(ACTION);

        Map<String, String> newOptions = new HashMap<>(context.getCatalogTable().getOptions());
        newOptions.remove(DELEGATE.key());
        newOptions.remove(ACTION.key());

        DynamicTableFactory.Context newContext =
                new FactoryUtil.DefaultDynamicTableContext(
                        context.getObjectIdentifier(),
                        context.getCatalogTable().copy(newOptions),
                        context.getEnrichmentOptions(),
                        context.getConfiguration(),
                        context.getClassLoader(),
                        context.isTemporary());

        DynamicTableSink delegate =
                FactoryUtil.discoverFactory(
                                context.getClassLoader(),
                                DynamicTableSinkFactory.class,
                                delegateFactoryIdentifier)
                        .createDynamicTableSink(newContext);

        RowDataConsumer action = MockActions.resolve(actionId, RowDataConsumer.class);
        action.open(context.getClassLoader(), context.getPhysicalRowDataType());

        return new DelegatingDynamicTableSink(delegate, action);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.singleton(ACTION);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    private static class DelegatingDynamicTableSink implements DynamicTableSink {

        private final DynamicTableSink delegate;
        private final RowDataConsumer action;

        private DelegatingDynamicTableSink(DynamicTableSink delegate, RowDataConsumer action) {
            this.delegate = delegate;
            this.action = action;
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
            return delegate.getChangelogMode(requestedMode);
        }

        @Override
        public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
            SinkRuntimeProvider runtimeProvider = delegate.getSinkRuntimeProvider(context);
            if (runtimeProvider instanceof DataStreamSinkProvider) {
                return (DataStreamSinkProvider)
                        dataStream ->
                                ((DataStreamSinkProvider) runtimeProvider)
                                        .consumeDataStream(
                                                dataStream.map(
                                                        (MapFunction<RowData, RowData>)
                                                                value -> {
                                                                    action.accept(value);
                                                                    return value;
                                                                },
                                                        dataStream.getType()));
            } else if (runtimeProvider instanceof SinkFunctionProvider) {
                return new DelegatingSinkFunctionProvider(
                        (SinkFunctionProvider) runtimeProvider, action);
            } else if (runtimeProvider instanceof OutputFormatProvider) {
                return new DelegatingOutputFormatProvider(
                        ((OutputFormatProvider) runtimeProvider), action);
            } else if (runtimeProvider instanceof SinkProvider) {
                return new DelegatingSinkProvider(((SinkProvider) runtimeProvider), action);
            } else {
                throw new TableException("Unsupported sink runtime provider.");
            }
        }

        @Override
        public DynamicTableSink copy() {
            return new DelegatingDynamicTableSink(delegate.copy(), action);
        }

        @Override
        public String asSummaryString() {
            return "Delegating sink with " + delegate.asSummaryString();
        }
    }

    private static class DelegatingSinkFunctionProvider implements SinkFunctionProvider {

        private final SinkFunctionProvider sinkFunctionProvider;
        private final RowDataConsumer action;

        public DelegatingSinkFunctionProvider(
                SinkFunctionProvider sinkFunctionProvider, RowDataConsumer action) {
            this.sinkFunctionProvider = sinkFunctionProvider;
            this.action = action;
        }

        @Override
        public SinkFunction<RowData> createSinkFunction() {
            final SinkFunction<RowData> delegate = sinkFunctionProvider.createSinkFunction();
            return new SinkFunction<RowData>() {
                @Override
                public void invoke(RowData value, Context context) throws Exception {
                    action.accept(value);
                    delegate.invoke(value, context);
                }

                @Override
                public void writeWatermark(Watermark watermark) throws Exception {
                    delegate.writeWatermark(watermark);
                }

                @Override
                public void finish() throws Exception {
                    delegate.finish();
                }
            };
        }

        @Override
        public Optional<Integer> getParallelism() {
            return sinkFunctionProvider.getParallelism();
        }
    }

    private static class DelegatingOutputFormatProvider implements OutputFormatProvider {

        private final OutputFormatProvider outputFormatProvider;
        private final RowDataConsumer action;

        public DelegatingOutputFormatProvider(
                OutputFormatProvider outputFormatProvider, RowDataConsumer action) {
            this.outputFormatProvider = outputFormatProvider;
            this.action = action;
        }

        @Override
        public OutputFormat<RowData> createOutputFormat() {
            final OutputFormat<RowData> delegate = outputFormatProvider.createOutputFormat();
            return new OutputFormat<RowData>() {
                @Override
                public void configure(Configuration parameters) {
                    delegate.configure(parameters);
                }

                @Override
                public void open(int taskNumber, int numTasks) throws IOException {
                    delegate.open(taskNumber, numTasks);
                }

                @Override
                public void writeRecord(RowData record) throws IOException {
                    action.accept(record);
                    delegate.writeRecord(record);
                }

                @Override
                public void close() throws IOException {
                    delegate.close();
                }
            };
        }

        @Override
        public Optional<Integer> getParallelism() {
            return outputFormatProvider.getParallelism();
        }
    }

    private static class DelegatingSinkProvider implements SinkProvider {

        private final SinkProvider sinkProvider;
        private final RowDataConsumer action;

        public DelegatingSinkProvider(SinkProvider sinkProvider, RowDataConsumer action) {
            this.sinkProvider = sinkProvider;
            this.action = action;
        }

        @Override
        public Optional<Integer> getParallelism() {
            return sinkProvider.getParallelism();
        }

        @Override
        @SuppressWarnings("unchecked")
        public Sink<RowData, ?, ?, ?> createSink() {
            final Sink<RowData, Object, Object, Object> sink =
                    (Sink<RowData, Object, Object, Object>) sinkProvider.createSink();
            return new Sink<RowData, Object, Object, Object>() {
                @Override
                public SinkWriter<RowData, Object, Object> createWriter(
                        InitContext context, List<Object> states) throws IOException {
                    final SinkWriter<RowData, Object, Object> sinkWriter =
                            sink.createWriter(context, states);
                    return new SinkWriter<RowData, Object, Object>() {
                        @Override
                        public void write(RowData element, Context context)
                                throws IOException, InterruptedException {
                            action.accept(element);
                            sinkWriter.write(element, context);
                        }

                        @Override
                        public List<Object> prepareCommit(boolean flush)
                                throws IOException, InterruptedException {
                            return sinkWriter.prepareCommit(flush);
                        }

                        @Override
                        public void close() throws Exception {
                            sinkWriter.close();
                        }

                        @Override
                        public void writeWatermark(Watermark watermark)
                                throws IOException, InterruptedException {
                            sinkWriter.writeWatermark(watermark);
                        }

                        @Override
                        public List<Object> snapshotState(long checkpointId) throws IOException {
                            return sinkWriter.snapshotState(checkpointId);
                        }

                        @Override
                        public List<Object> snapshotState() throws IOException {
                            return sinkWriter.snapshotState();
                        }
                    };
                }

                @Override
                public Optional<SimpleVersionedSerializer<Object>> getWriterStateSerializer() {
                    return sink.getWriterStateSerializer();
                }

                @Override
                public Optional<Committer<Object>> createCommitter() throws IOException {
                    return sink.createCommitter();
                }

                @Override
                public Optional<GlobalCommitter<Object, Object>> createGlobalCommitter()
                        throws IOException {
                    return sink.createGlobalCommitter();
                }

                @Override
                public Optional<SimpleVersionedSerializer<Object>> getCommittableSerializer() {
                    return sink.getCommittableSerializer();
                }

                @Override
                public Optional<SimpleVersionedSerializer<Object>>
                        getGlobalCommittableSerializer() {
                    return sink.getGlobalCommittableSerializer();
                }
            };
        }
    }
}
