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

package org.apache.flink.runtime.operators.util;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BloomFilterTest {

    private static BloomFilter bloomFilter;
    private static final int INPUT_SIZE = 1024;
    private static final double FALSE_POSITIVE_PROBABILITY = 0.05;

    @BeforeClass
    public static void init() {
        int bitsSize = BloomFilter.optimalNumOfBits(INPUT_SIZE, FALSE_POSITIVE_PROBABILITY);
        bitsSize = bitsSize + (Long.SIZE - (bitsSize % Long.SIZE));
        int byteSize = bitsSize >>> 3;
        MemorySegment memorySegment = MemorySegmentFactory.allocateUnpooledSegment(byteSize);
        bloomFilter = new BloomFilter(INPUT_SIZE, byteSize);
        bloomFilter.setBitsLocation(memorySegment, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBloomFilterArguments1() {
        new BloomFilter(-1, 128);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBloomFilterArguments2() {
        new BloomFilter(0, 128);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBloomFilterArguments3() {
        new BloomFilter(1024, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBloomFilterArguments4() {
        new BloomFilter(1024, 0);
    }

    @Test
    public void testBloomNumBits() {
        assertThat(BloomFilter.optimalNumOfBits(0, 0)).isEqualTo(0);
        assertThat(BloomFilter.optimalNumOfBits(0, 1)).isEqualTo(0);
        assertThat(BloomFilter.optimalNumOfBits(1, 1)).isEqualTo(0);
        assertThat(BloomFilter.optimalNumOfBits(1, 0.03)).isEqualTo(7);
        assertThat(BloomFilter.optimalNumOfBits(10, 0.03)).isEqualTo(72);
        assertThat(BloomFilter.optimalNumOfBits(100, 0.03)).isEqualTo(729);
        assertThat(BloomFilter.optimalNumOfBits(1000, 0.03)).isEqualTo(7298);
        assertThat(BloomFilter.optimalNumOfBits(10000, 0.03)).isEqualTo(72984);
        assertThat(BloomFilter.optimalNumOfBits(100000, 0.03)).isEqualTo(729844);
        assertThat(BloomFilter.optimalNumOfBits(1000000, 0.03)).isEqualTo(7298440);
        assertThat(BloomFilter.optimalNumOfBits(1000000, 0.05)).isEqualTo(6235224);
    }

    @Test
    public void testBloomFilterNumHashFunctions() {
        assertThat(BloomFilter.optimalNumOfHashFunctions(-1, -1)).isEqualTo(1);
        assertThat(BloomFilter.optimalNumOfHashFunctions(0, 0)).isEqualTo(1);
        assertThat(BloomFilter.optimalNumOfHashFunctions(10, 0)).isEqualTo(1);
        assertThat(BloomFilter.optimalNumOfHashFunctions(10, 10)).isEqualTo(1);
        assertThat(BloomFilter.optimalNumOfHashFunctions(10, 100)).isEqualTo(7);
        assertThat(BloomFilter.optimalNumOfHashFunctions(100, 100)).isEqualTo(1);
        assertThat(BloomFilter.optimalNumOfHashFunctions(1000, 100)).isEqualTo(1);
        assertThat(BloomFilter.optimalNumOfHashFunctions(10000, 100)).isEqualTo(1);
        assertThat(BloomFilter.optimalNumOfHashFunctions(100000, 100)).isEqualTo(1);
        assertThat(BloomFilter.optimalNumOfHashFunctions(1000000, 100)).isEqualTo(1);
    }

    @Test
    public void testBloomFilterFalsePositiveProbability() {
        assertThat(BloomFilter.optimalNumOfBits(1000000, 0.03)).isEqualTo(7298440);
        assertThat(BloomFilter.optimalNumOfBits(1000000, 0.05)).isEqualTo(6235224);
        assertThat(BloomFilter.optimalNumOfBits(1000000, 0.1)).isEqualTo(4792529);
        assertThat(BloomFilter.optimalNumOfBits(1000000, 0.2)).isEqualTo(3349834);
        assertThat(BloomFilter.optimalNumOfBits(1000000, 0.3)).isEqualTo(2505911);
        assertThat(BloomFilter.optimalNumOfBits(1000000, 0.4)).isEqualTo(1907139);

        // Make sure the estimated fpp error is less than 1%.
        assertThat(
                        Math.abs(
                                        BloomFilter.estimateFalsePositiveProbability(
                                                        1000000, 7298440)
                                                - 0.03)
                                < 0.01)
                .isTrue();
        assertThat(
                        Math.abs(
                                        BloomFilter.estimateFalsePositiveProbability(
                                                        1000000, 6235224)
                                                - 0.05)
                                < 0.01)
                .isTrue();
        assertThat(
                        Math.abs(
                                        BloomFilter.estimateFalsePositiveProbability(
                                                        1000000, 4792529)
                                                - 0.1)
                                < 0.01)
                .isTrue();
        assertThat(
                        Math.abs(
                                        BloomFilter.estimateFalsePositiveProbability(
                                                        1000000, 3349834)
                                                - 0.2)
                                < 0.01)
                .isTrue();
        assertThat(
                        Math.abs(
                                        BloomFilter.estimateFalsePositiveProbability(
                                                        1000000, 2505911)
                                                - 0.3)
                                < 0.01)
                .isTrue();
        assertThat(
                        Math.abs(
                                        BloomFilter.estimateFalsePositiveProbability(
                                                        1000000, 1907139)
                                                - 0.4)
                                < 0.01)
                .isTrue();
    }

    @Test
    public void testHashcodeInput() {
        bloomFilter.reset();
        int val1 = "val1".hashCode();
        int val2 = "val2".hashCode();
        int val3 = "val3".hashCode();
        int val4 = "val4".hashCode();
        int val5 = "val5".hashCode();

        assertThat(bloomFilter.testHash(val1)).isFalse();
        assertThat(bloomFilter.testHash(val2)).isFalse();
        assertThat(bloomFilter.testHash(val3)).isFalse();
        assertThat(bloomFilter.testHash(val4)).isFalse();
        assertThat(bloomFilter.testHash(val5)).isFalse();
        bloomFilter.addHash(val1);
        assertThat(bloomFilter.testHash(val1)).isTrue();
        assertThat(bloomFilter.testHash(val2)).isFalse();
        assertThat(bloomFilter.testHash(val3)).isFalse();
        assertThat(bloomFilter.testHash(val4)).isFalse();
        assertThat(bloomFilter.testHash(val5)).isFalse();
        bloomFilter.addHash(val2);
        assertThat(bloomFilter.testHash(val1)).isTrue();
        assertThat(bloomFilter.testHash(val2)).isTrue();
        assertThat(bloomFilter.testHash(val3)).isFalse();
        assertThat(bloomFilter.testHash(val4)).isFalse();
        assertThat(bloomFilter.testHash(val5)).isFalse();
        bloomFilter.addHash(val3);
        assertThat(bloomFilter.testHash(val1)).isTrue();
        assertThat(bloomFilter.testHash(val2)).isTrue();
        assertThat(bloomFilter.testHash(val3)).isTrue();
        assertThat(bloomFilter.testHash(val4)).isFalse();
        assertThat(bloomFilter.testHash(val5)).isFalse();
        bloomFilter.addHash(val4);
        assertThat(bloomFilter.testHash(val1)).isTrue();
        assertThat(bloomFilter.testHash(val2)).isTrue();
        assertThat(bloomFilter.testHash(val3)).isTrue();
        assertThat(bloomFilter.testHash(val4)).isTrue();
        assertThat(bloomFilter.testHash(val5)).isFalse();
        bloomFilter.addHash(val5);
        assertThat(bloomFilter.testHash(val1)).isTrue();
        assertThat(bloomFilter.testHash(val2)).isTrue();
        assertThat(bloomFilter.testHash(val3)).isTrue();
        assertThat(bloomFilter.testHash(val4)).isTrue();
        assertThat(bloomFilter.testHash(val5)).isTrue();
    }
}
