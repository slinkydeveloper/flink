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

package org.apache.flink.graph.pregel;

import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.DeltaIterationResultSet;
import org.apache.flink.api.java.operators.SingleInputUdfOperator;
import org.apache.flink.api.java.operators.TwoInputUdfOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test the creation of a {@link VertexCentricIteration} program. */
@SuppressWarnings("serial")
public class PregelTranslationTest {

    private static final String ITERATION_NAME = "Test Name";

    private static final String AGGREGATOR_NAME = "AggregatorName";

    private static final String BC_SET_NAME = "broadcast messages";

    private static final int NUM_ITERATIONS = 13;

    private static final int ITERATION_parallelism = 77;

    @Test
    public void testTranslationPlainEdges() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Long> bcVar = env.fromElements(1L);

        DataSet<Vertex<String, Double>> result;

        // ------------ construct the test program ------------------

        DataSet<Tuple2<String, Double>> initialVertices =
                env.fromElements(new Tuple2<>("abc", 3.44));

        DataSet<Tuple2<String, String>> edges = env.fromElements(new Tuple2<>("a", "c"));

        Graph<String, Double, NullValue> graph =
                Graph.fromTupleDataSet(
                        initialVertices,
                        edges.map(
                                new MapFunction<
                                        Tuple2<String, String>,
                                        Tuple3<String, String, NullValue>>() {

                                    public Tuple3<String, String, NullValue> map(
                                            Tuple2<String, String> edge) {
                                        return new Tuple3<>(
                                                edge.f0, edge.f1, NullValue.getInstance());
                                    }
                                }),
                        env);

        VertexCentricConfiguration parameters = new VertexCentricConfiguration();

        parameters.addBroadcastSet(BC_SET_NAME, bcVar);
        parameters.setName(ITERATION_NAME);
        parameters.setParallelism(ITERATION_parallelism);
        parameters.registerAggregator(AGGREGATOR_NAME, new LongSumAggregator());

        result =
                graph.runVertexCentricIteration(new MyCompute(), null, NUM_ITERATIONS, parameters)
                        .getVertices();

        result.output(new DiscardingOutputFormat<>());

        // ------------- validate the java program ----------------

        assertThat(result).isInstanceOf(DeltaIterationResultSet.class);

        DeltaIterationResultSet<?, ?> resultSet = (DeltaIterationResultSet<?, ?>) result;
        DeltaIteration<?, ?> iteration = resultSet.getIterationHead();

        // check the basic iteration properties
        assertThat(resultSet.getMaxIterations()).isEqualTo(NUM_ITERATIONS);
        assertThat(resultSet.getKeyPositions()).isEqualTo(new int[] {0});
        assertThat(iteration.getParallelism()).isEqualTo(ITERATION_parallelism);
        assertThat(iteration.getName()).isEqualTo(ITERATION_NAME);

        assertThat(
                        iteration
                                .getAggregators()
                                .getAllRegisteredAggregators()
                                .iterator()
                                .next()
                                .getName())
                .isEqualTo(AGGREGATOR_NAME);

        TwoInputUdfOperator<?, ?, ?, ?> computationCoGroup =
                (TwoInputUdfOperator<?, ?, ?, ?>)
                        ((SingleInputUdfOperator<?, ?, ?>) resultSet.getNextWorkset()).getInput();

        // validate that the broadcast sets are forwarded
        assertThat(computationCoGroup.getBroadcastSets().get(BC_SET_NAME)).isEqualTo(bcVar);
    }

    // --------------------------------------------------------------------------------------------

    private static final class MyCompute
            extends ComputeFunction<String, Double, NullValue, Double> {

        @Override
        public void compute(Vertex<String, Double> vertex, MessageIterator<Double> messages)
                throws Exception {}
    }
}
