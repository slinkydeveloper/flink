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

package org.apache.flink.streaming.api;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.StatefulSequenceSource;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.SplittableIterator;

import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link StreamExecutionEnvironment}. */
public class StreamExecutionEnvironmentTest {

    @Test
    public void fromElementsWithBaseTypeTest1() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(ParentClass.class, new SubClass(1, "Java"), new ParentClass(1, "hello"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void fromElementsWithBaseTypeTest2() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(SubClass.class, new SubClass(1, "Java"), new ParentClass(1, "hello"));
    }

    @Test
    public void testFromElementsDeducedType() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.fromElements("a", "b");

        FromElementsFunction<String> elementsFunction =
                (FromElementsFunction<String>) getFunctionFromDataSource(source);
        assertThat(elementsFunction.getSerializer())
                .isEqualTo(BasicTypeInfo.STRING_TYPE_INFO.createSerializer(env.getConfig()));
    }

    @Test
    public void testFromElementsPostConstructionType() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.fromElements("a", "b");
        TypeInformation<String> customType = new GenericTypeInfo<>(String.class);

        source.returns(customType);

        FromElementsFunction<String> elementsFunction =
                (FromElementsFunction<String>) getFunctionFromDataSource(source);
        assertThat(elementsFunction.getSerializer())
                .isEqualTo(BasicTypeInfo.STRING_TYPE_INFO.createSerializer(env.getConfig()));
        assertThat(elementsFunction.getSerializer())
                .isEqualTo(customType.createSerializer(env.getConfig()));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testFromCollectionParallelism() {
        try {
            TypeInformation<Integer> typeInfo = BasicTypeInfo.INT_TYPE_INFO;
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStreamSource<Integer> dataStream1 =
                    env.fromCollection(new DummySplittableIterator<Integer>(), typeInfo);

            try {
                dataStream1.setParallelism(4);
                fail("should throw an exception");
            } catch (IllegalArgumentException e) {
                // expected
            }

            dataStream1.addSink(new DiscardingSink<Integer>());

            DataStreamSource<Integer> dataStream2 =
                    env.fromParallelCollection(new DummySplittableIterator<Integer>(), typeInfo)
                            .setParallelism(4);

            dataStream2.addSink(new DiscardingSink<Integer>());

            final StreamGraph streamGraph = env.getStreamGraph();
            streamGraph.getStreamingPlanAsJSON();

            assertThat(streamGraph.getStreamNode(dataStream1.getId()).getParallelism())
                    .as("Parallelism of collection source must be 1.")
                    .isEqualTo(1);
            assertThat(streamGraph.getStreamNode(dataStream2.getId()).getParallelism())
                    .as("Parallelism of parallel collection source must be 4.")
                    .isEqualTo(4);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testSources() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SourceFunction<Integer> srcFun =
                new SourceFunction<Integer>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {}

                    @Override
                    public void cancel() {}
                };
        DataStreamSource<Integer> src1 = env.addSource(srcFun);
        src1.addSink(new DiscardingSink<Integer>());
        assertThat(getFunctionFromDataSource(src1)).isEqualTo(srcFun);

        List<Long> list = Arrays.asList(0L, 1L, 2L);

        DataStreamSource<Long> src2 = env.generateSequence(0, 2);
        assertThat(getFunctionFromDataSource(src2)).isInstanceOf(StatefulSequenceSource.class);

        DataStreamSource<Long> src3 = env.fromElements(0L, 1L, 2L);
        assertThat(getFunctionFromDataSource(src3)).isInstanceOf(FromElementsFunction.class);

        DataStreamSource<Long> src4 = env.fromCollection(list);
        assertThat(getFunctionFromDataSource(src4)).isInstanceOf(FromElementsFunction.class);
    }

    /** Verifies that the API method doesn't throw and creates a source of the expected type. */
    @Test
    public void testFromSequence() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> src = env.fromSequence(0, 2);

        assertThat(src.getType()).isEqualTo(BasicTypeInfo.LONG_TYPE_INFO);
    }

    @Test
    public void testParallelismBounds() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SourceFunction<Integer> srcFun =
                new SourceFunction<Integer>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {}

                    @Override
                    public void cancel() {}
                };

        SingleOutputStreamOperator<Object> operator =
                env.addSource(srcFun)
                        .flatMap(
                                new FlatMapFunction<Integer, Object>() {

                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public void flatMap(Integer value, Collector<Object> out)
                                            throws Exception {}
                                });

        // default value for max parallelism
        assertThat(operator.getTransformation().getMaxParallelism()).isEqualTo(-1);

        // bounds for parallelism 1
        try {
            operator.setParallelism(0);
            fail("unknown failure");
        } catch (IllegalArgumentException expected) {
        }

        // bounds for parallelism 2
        operator.setParallelism(1);
        assertThat(operator.getParallelism()).isEqualTo(1);

        // bounds for parallelism 3
        operator.setParallelism(1 << 15);
        assertThat(operator.getParallelism()).isEqualTo(1 << 15);

        // default value after generating
        env.getStreamGraph(false).getJobGraph();
        assertThat(operator.getTransformation().getMaxParallelism()).isEqualTo(-1);

        // configured value after generating
        env.setMaxParallelism(42);
        env.getStreamGraph(false).getJobGraph();
        assertThat(operator.getTransformation().getMaxParallelism()).isEqualTo(42);

        // bounds configured parallelism 1
        try {
            env.setMaxParallelism(0);
            fail("unknown failure");
        } catch (IllegalArgumentException expected) {
        }

        // bounds configured parallelism 2
        try {
            env.setMaxParallelism(1 + (1 << 15));
            fail("unknown failure");
        } catch (IllegalArgumentException expected) {
        }

        // bounds for max parallelism 1
        try {
            operator.setMaxParallelism(0);
            fail("unknown failure");
        } catch (IllegalArgumentException expected) {
        }

        // bounds for max parallelism 2
        try {
            operator.setMaxParallelism(1 + (1 << 15));
            fail("unknown failure");
        } catch (IllegalArgumentException expected) {
        }

        // bounds for max parallelism 3
        operator.setMaxParallelism(1);
        assertThat(operator.getTransformation().getMaxParallelism()).isEqualTo(1);

        // bounds for max parallelism 4
        operator.setMaxParallelism(1 << 15);
        assertThat(operator.getTransformation().getMaxParallelism()).isEqualTo(1 << 15);

        // override config
        env.getStreamGraph(false).getJobGraph();
        assertThat(operator.getTransformation().getMaxParallelism()).isEqualTo(1 << 15);
    }

    @Test
    public void testRegisterSlotSharingGroup() {
        final SlotSharingGroup ssg1 =
                SlotSharingGroup.newBuilder("ssg1").setCpuCores(1).setTaskHeapMemoryMB(100).build();
        final SlotSharingGroup ssg2 =
                SlotSharingGroup.newBuilder("ssg2").setCpuCores(2).setTaskHeapMemoryMB(200).build();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.registerSlotSharingGroup(ssg1);
        env.registerSlotSharingGroup(ssg2);
        env.registerSlotSharingGroup(SlotSharingGroup.newBuilder("ssg3").build());

        final DataStream<Integer> source = env.fromElements(1).slotSharingGroup("ssg1");
        source.map(value -> value).slotSharingGroup(ssg2).addSink(new DiscardingSink<>());

        final StreamGraph streamGraph = env.getStreamGraph();
        assertThat(streamGraph.getSlotSharingGroupResource("ssg1").get())
                .isEqualTo(ResourceProfile.fromResources(1, 100));
        assertThat(streamGraph.getSlotSharingGroupResource("ssg2").get())
                .isEqualTo(ResourceProfile.fromResources(2, 200));
        assertThat(streamGraph.getSlotSharingGroupResource("ssg3").isPresent()).isFalse();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRegisterSlotSharingGroupConflict() {
        final SlotSharingGroup ssg =
                SlotSharingGroup.newBuilder("ssg1").setCpuCores(1).setTaskHeapMemoryMB(100).build();
        final SlotSharingGroup ssgConflict =
                SlotSharingGroup.newBuilder("ssg1").setCpuCores(2).setTaskHeapMemoryMB(200).build();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.registerSlotSharingGroup(ssg);

        final DataStream<Integer> source = env.fromElements(1).slotSharingGroup("ssg1");
        source.map(value -> value).slotSharingGroup(ssgConflict).addSink(new DiscardingSink<>());

        env.getStreamGraph();
    }

    @Test
    public void testGetStreamGraph() {
        try {
            TypeInformation<Integer> typeInfo = BasicTypeInfo.INT_TYPE_INFO;
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStreamSource<Integer> dataStream1 =
                    env.fromCollection(new DummySplittableIterator<Integer>(), typeInfo);
            dataStream1.addSink(new DiscardingSink<Integer>());
            assertThat(env.getStreamGraph().getStreamNodes().size()).isEqualTo(2);

            DataStreamSource<Integer> dataStream2 =
                    env.fromCollection(new DummySplittableIterator<Integer>(), typeInfo);
            dataStream2.addSink(new DiscardingSink<Integer>());
            assertThat(env.getStreamGraph().getStreamNodes().size()).isEqualTo(2);

            DataStreamSource<Integer> dataStream3 =
                    env.fromCollection(new DummySplittableIterator<Integer>(), typeInfo);
            dataStream3.addSink(new DiscardingSink<Integer>());
            // Does not clear the transformations.
            env.getExecutionPlan();
            DataStreamSource<Integer> dataStream4 =
                    env.fromCollection(new DummySplittableIterator<Integer>(), typeInfo);
            dataStream4.addSink(new DiscardingSink<Integer>());
            assertThat(env.getStreamGraph().getStreamNodes().size()).isEqualTo(4);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testDefaultJobName() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        testJobName(StreamGraphGenerator.DEFAULT_STREAMING_JOB_NAME, env);

        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        testJobName(StreamGraphGenerator.DEFAULT_BATCH_JOB_NAME, env);
    }

    @Test
    public void testUserDefinedJobName() {
        String jobName = "MyTestJob";
        Configuration config = new Configuration();
        config.set(PipelineOptions.NAME, jobName);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        testJobName(jobName, env);
    }

    @Test
    public void testUserDefinedJobNameWithConfigure() {
        String jobName = "MyTestJob";
        Configuration config = new Configuration();
        config.set(PipelineOptions.NAME, jobName);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.configure(config, this.getClass().getClassLoader());
        testJobName(jobName, env);
    }

    private void testJobName(String expectedJobName, StreamExecutionEnvironment env) {
        env.fromElements(1, 2, 3).print();
        StreamGraph streamGraph = env.getStreamGraph();
        assertThat(streamGraph.getJobName()).isEqualTo(expectedJobName);
    }

    @Test
    public void testAddSourceWithUserDefinedTypeInfo() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Row> source1 =
                env.addSource(new RowSourceFunction(), Types.ROW(Types.STRING));
        // the source type information should be the user defined type
        assertThat(source1.getType()).isEqualTo(Types.ROW(Types.STRING));

        DataStreamSource<Row> source2 = env.addSource(new RowSourceFunction());
        // the source type information should be derived from RowSourceFunction#getProducedType
        assertThat(source2.getType()).isEqualTo(new GenericTypeInfo<>(Row.class));
    }

    /////////////////////////////////////////////////////////////
    // Utilities
    /////////////////////////////////////////////////////////////

    private static StreamOperator<?> getOperatorFromDataStream(DataStream<?> dataStream) {
        StreamExecutionEnvironment env = dataStream.getExecutionEnvironment();
        StreamGraph streamGraph = env.getStreamGraph();
        return streamGraph.getStreamNode(dataStream.getId()).getOperator();
    }

    @SuppressWarnings("unchecked")
    private static <T> SourceFunction<T> getFunctionFromDataSource(
            DataStreamSource<T> dataStreamSource) {
        dataStreamSource.addSink(new DiscardingSink<T>());
        AbstractUdfStreamOperator<?, ?> operator =
                (AbstractUdfStreamOperator<?, ?>) getOperatorFromDataStream(dataStreamSource);
        return (SourceFunction<T>) operator.getUserFunction();
    }

    private static class DummySplittableIterator<T> extends SplittableIterator<T> {
        private static final long serialVersionUID = 1312752876092210499L;

        @SuppressWarnings("unchecked")
        @Override
        public Iterator<T>[] split(int numPartitions) {
            return (Iterator<T>[]) new Iterator<?>[0];
        }

        @Override
        public int getMaximumNumberOfSplits() {
            return 0;
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public T next() {
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private static class ParentClass {
        int num;
        String string;

        public ParentClass(int num, String string) {
            this.num = num;
            this.string = string;
        }
    }

    private static class SubClass extends ParentClass {
        public SubClass(int num, String string) {
            super(num, string);
        }
    }

    private static class RowSourceFunction
            implements SourceFunction<Row>, ResultTypeQueryable<Row> {
        private static final long serialVersionUID = 5216362688122691404L;

        @Override
        public TypeInformation<Row> getProducedType() {
            return TypeInformation.of(Row.class);
        }

        @Override
        public void run(SourceContext<Row> ctx) throws Exception {}

        @Override
        public void cancel() {}
    }
}
