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

package org.apache.flink.test.util;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.summarize.BooleanColumnSummary;
import org.apache.flink.api.java.summarize.NumericColumnSummary;
import org.apache.flink.api.java.summarize.StringColumnSummary;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.test.operators.util.CollectionDataSets;
import org.apache.flink.types.DoubleValue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/** Integration tests for {@link DataSetUtils}. */
@RunWith(Parameterized.class)
public class DataSetUtilsITCase extends MultipleProgramsTestBase {

    public DataSetUtilsITCase(TestExecutionMode mode) {
        super(mode);
    }

    @Test
    public void testCountElementsPerPartition() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        long expectedSize = 100L;
        DataSet<Long> numbers = env.generateSequence(0, expectedSize - 1);

        DataSet<Tuple2<Integer, Long>> ds = DataSetUtils.countElementsPerPartition(numbers);

        assertThat(ds.count()).isEqualTo(env.getParallelism());
        assertThat(ds.sum(1).collect().get(0).f1.longValue()).isEqualTo(expectedSize);
    }

    @Test
    public void testZipWithIndex() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        long expectedSize = 100L;
        DataSet<Long> numbers = env.generateSequence(0, expectedSize - 1);

        List<Tuple2<Long, Long>> result =
                new ArrayList<>(DataSetUtils.zipWithIndex(numbers).collect());

        assertThat(result.size()).isEqualTo(expectedSize);
        // sort result by created index
        Collections.sort(
                result,
                new Comparator<Tuple2<Long, Long>>() {
                    @Override
                    public int compare(Tuple2<Long, Long> o1, Tuple2<Long, Long> o2) {
                        return o1.f0.compareTo(o2.f0);
                    }
                });
        // test if index is consecutive
        for (int i = 0; i < expectedSize; i++) {
            assertThat(result.get(i).f0.longValue()).isEqualTo(i);
        }
    }

    @Test
    public void testZipWithUniqueId() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        long expectedSize = 100L;
        DataSet<Long> numbers = env.generateSequence(1L, expectedSize);

        DataSet<Long> ids =
                DataSetUtils.zipWithUniqueId(numbers)
                        .map(
                                new MapFunction<Tuple2<Long, Long>, Long>() {
                                    @Override
                                    public Long map(Tuple2<Long, Long> value) throws Exception {
                                        return value.f0;
                                    }
                                });

        Set<Long> result = new HashSet<>(ids.collect());

        assertThat(result.size()).isEqualTo(expectedSize);
    }

    @Test
    public void testIntegerDataSetChecksumHashCode() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Integer> ds = CollectionDataSets.getIntegerDataSet(env);

        Utils.ChecksumHashCode checksum = DataSetUtils.checksumHashCode(ds);
        assertThat(15).isEqualTo(checksum.getCount());
        assertThat(55).isEqualTo(checksum.getChecksum());
    }

    @Test
    public void testSummarize() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Tuple8<Short, Integer, Long, Float, Double, String, Boolean, DoubleValue>> data =
                new ArrayList<>();
        data.add(
                new Tuple8<>(
                        (short) 1, 1, 100L, 0.1f, 1.012376, "hello", false, new DoubleValue(50.0)));
        data.add(
                new Tuple8<>(
                        (short) 2, 2, 1000L, 0.2f, 2.003453, "hello", true, new DoubleValue(50.0)));
        data.add(
                new Tuple8<>(
                        (short) 4,
                        10,
                        10000L,
                        0.2f,
                        75.00005,
                        "null",
                        true,
                        new DoubleValue(50.0)));
        data.add(new Tuple8<>((short) 10, 4, 100L, 0.9f, 79.5, "", true, new DoubleValue(50.0)));
        data.add(
                new Tuple8<>(
                        (short) 5, 5, 1000L, 0.2f, 10.0000001, "a", false, new DoubleValue(50.0)));
        data.add(
                new Tuple8<>(
                        (short) 6,
                        6,
                        10L,
                        0.1f,
                        0.0000000000023,
                        "",
                        true,
                        new DoubleValue(100.0)));
        data.add(
                new Tuple8<>(
                        (short) 7,
                        7,
                        1L,
                        0.2f,
                        Double.POSITIVE_INFINITY,
                        "abcdefghijklmnop",
                        true,
                        new DoubleValue(100.0)));
        data.add(
                new Tuple8<>(
                        (short) 8,
                        8,
                        -100L,
                        0.001f,
                        Double.NaN,
                        "abcdefghi",
                        true,
                        new DoubleValue(100.0)));

        Collections.shuffle(data);

        DataSet<Tuple8<Short, Integer, Long, Float, Double, String, Boolean, DoubleValue>> ds =
                env.fromCollection(data);

        // call method under test
        Tuple results = DataSetUtils.summarize(ds);

        assertThat(results.getArity()).isEqualTo(8);

        NumericColumnSummary<Short> col0Summary = results.getField(0);
        assertThat(col0Summary.getNonMissingCount()).isEqualTo(8);
        assertThat(col0Summary.getMin().shortValue()).isEqualTo(1);
        assertThat(col0Summary.getMax().shortValue()).isEqualTo(10);
        assertThat(col0Summary.getMean().doubleValue()).isEqualTo(5.375);

        NumericColumnSummary<Integer> col1Summary = results.getField(1);
        assertThat(col1Summary.getMin().intValue()).isEqualTo(1);
        assertThat(col1Summary.getMax().intValue()).isEqualTo(10);
        assertThat(col1Summary.getMean().doubleValue()).isEqualTo(5.375);

        NumericColumnSummary<Long> col2Summary = results.getField(2);
        assertThat(col2Summary.getMin().longValue()).isEqualTo(-100L);
        assertThat(col2Summary.getMax().longValue()).isEqualTo(10000L);

        NumericColumnSummary<Float> col3Summary = results.getField(3);
        assertThat(col3Summary.getTotalCount()).isEqualTo(8);
        assertThat(col3Summary.getMin().doubleValue()).isCloseTo(0.001000, within(0.0000001));
        assertThat(col3Summary.getMax().doubleValue()).isCloseTo(0.89999999, within(0.0000001));
        assertThat(col3Summary.getMean().doubleValue())
                .isCloseTo(0.2376249988883501, within(0.000000000001));
        assertThat(col3Summary.getVariance().doubleValue())
                .isCloseTo(0.0768965488108089, within(0.00000001));
        assertThat(col3Summary.getStandardDeviation().doubleValue())
                .isCloseTo(0.27730226975415995, within(0.000000000001));

        NumericColumnSummary<Double> col4Summary = results.getField(4);
        assertThat(col4Summary.getNonMissingCount()).isEqualTo(6);
        assertThat(col4Summary.getMissingCount()).isEqualTo(2);
        assertThat(col4Summary.getMin().doubleValue()).isEqualTo(0.0000000000023);
        assertThat(col4Summary.getMax().doubleValue()).isCloseTo(79.5, within(0.000000000001));

        StringColumnSummary col5Summary = results.getField(5);
        assertThat(col5Summary.getTotalCount()).isEqualTo(8);
        assertThat(col5Summary.getNullCount()).isEqualTo(0);
        assertThat(col5Summary.getNonNullCount()).isEqualTo(8);
        assertThat(col5Summary.getEmptyCount()).isEqualTo(2);
        assertThat(col5Summary.getMinLength().intValue()).isEqualTo(0);
        assertThat(col5Summary.getMaxLength().intValue()).isEqualTo(16);
        assertThat(col5Summary.getMeanLength().doubleValue()).isCloseTo(5.0, within(0.0001));

        BooleanColumnSummary col6Summary = results.getField(6);
        assertThat(col6Summary.getTotalCount()).isEqualTo(8);
        assertThat(col6Summary.getFalseCount()).isEqualTo(2);
        assertThat(col6Summary.getTrueCount()).isEqualTo(6);
        assertThat(col6Summary.getNullCount()).isEqualTo(0);

        NumericColumnSummary<Double> col7Summary = results.getField(7);
        assertThat(col7Summary.getMax().doubleValue()).isCloseTo(100.0, within(0.00001));
        assertThat(col7Summary.getMin().doubleValue()).isCloseTo(50.0, within(0.00001));
    }
}
