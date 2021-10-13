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

package org.apache.flink.table.planner.benchmarks;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.util.Collector;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

/** Upcasting benchmark. */
public class UpcastingWithPrimitivesBenchmark {

    @State(Scope.Thread)
    public static class MyState {

        private RowData rowData;
        private FlatMapFunction<RowData, RowData> function;

        @Setup(Level.Trial)
        public void doSetup() {
            this.rowData = GenericRowData.of(Integer.MAX_VALUE);
            this.function =
                    CodeGenerationBenchmarkUtils.generateCastFunction(
                            new IntType(true), new BigIntType(true));
        }
    }

    final class MockCollector implements Collector<RowData> {
        private RowData record;

        MockCollector() {}

        @Override
        public void collect(RowData record) {
            this.record = record;
        }

        @Override
        public void close() {}
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void test(MyState myState, Blackhole blackhole) throws Exception {
        MockCollector collector = new MockCollector();
        myState.function.flatMap(myState.rowData, collector);

        // Make sure DCE don't kick in
        blackhole.consume(collector.record);
    }
}
