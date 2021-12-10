/*

* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.

*/

package org.apache.flink.runtime.state.metrics;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;

import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LatencyTrackingMapState}. */
public class LatencyTrackingMapStateTest extends LatencyTrackingStateTestBase<Integer> {
    @Override
    @SuppressWarnings("unchecked")
    MapStateDescriptor<Integer, Double> getStateDescriptor() {
        return new MapStateDescriptor<>("map", Integer.class, Double.class);
    }

    @Override
    TypeSerializer<Integer> getKeySerializer() {
        return IntSerializer.INSTANCE;
    }

    @Override
    void setCurrentKey(AbstractKeyedStateBackend<Integer> keyedBackend) {
        keyedBackend.setCurrentKey(1);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testLatencyTrackingMapState() throws Exception {
        AbstractKeyedStateBackend<Integer> keyedBackend = createKeyedBackend(getKeySerializer());
        try {
            LatencyTrackingMapState<Integer, VoidNamespace, Long, Double> latencyTrackingState =
                    (LatencyTrackingMapState)
                            createLatencyTrackingState(keyedBackend, getStateDescriptor());
            latencyTrackingState.setCurrentNamespace(VoidNamespace.INSTANCE);
            LatencyTrackingMapState.MapStateLatencyMetrics latencyTrackingStateMetric =
                    latencyTrackingState.getLatencyTrackingStateMetric();

            assertThat(latencyTrackingStateMetric.getContainsCount()).isEqualTo(0);
            assertThat(latencyTrackingStateMetric.getEntriesInitCount()).isEqualTo(0);
            assertThat(latencyTrackingStateMetric.getGetCount()).isEqualTo(0);
            assertThat(latencyTrackingStateMetric.getIsEmptyCount()).isEqualTo(0);
            assertThat(latencyTrackingStateMetric.getIteratorInitCount()).isEqualTo(0);
            assertThat(latencyTrackingStateMetric.getIteratorHasNextCount()).isEqualTo(0);
            assertThat(latencyTrackingStateMetric.getIteratorNextCount()).isEqualTo(0);
            assertThat(latencyTrackingStateMetric.getKeysInitCount()).isEqualTo(0);
            assertThat(latencyTrackingStateMetric.getValuesInitCount()).isEqualTo(0);
            assertThat(latencyTrackingStateMetric.getIteratorRemoveCount()).isEqualTo(0);
            assertThat(latencyTrackingStateMetric.getPutAllCount()).isEqualTo(0);
            assertThat(latencyTrackingStateMetric.getPutCount()).isEqualTo(0);
            assertThat(latencyTrackingStateMetric.getRemoveCount()).isEqualTo(0);

            setCurrentKey(keyedBackend);
            ThreadLocalRandom random = ThreadLocalRandom.current();
            for (int index = 1; index <= SAMPLE_INTERVAL; index++) {
                int expectedResult = index == SAMPLE_INTERVAL ? 0 : index;
                latencyTrackingState.put(random.nextLong(), random.nextDouble());
                assertThat(latencyTrackingStateMetric.getPutCount()).isEqualTo(expectedResult);

                latencyTrackingState.putAll(
                        Collections.singletonMap(random.nextLong(), random.nextDouble()));
                assertThat(latencyTrackingStateMetric.getPutAllCount()).isEqualTo(expectedResult);

                latencyTrackingState.get(random.nextLong());
                assertThat(latencyTrackingStateMetric.getGetCount()).isEqualTo(expectedResult);

                latencyTrackingState.remove(random.nextLong());
                assertThat(latencyTrackingStateMetric.getRemoveCount()).isEqualTo(expectedResult);

                latencyTrackingState.contains(random.nextLong());
                assertThat(latencyTrackingStateMetric.getContainsCount()).isEqualTo(expectedResult);

                latencyTrackingState.isEmpty();
                assertThat(latencyTrackingStateMetric.getIsEmptyCount()).isEqualTo(expectedResult);

                latencyTrackingState.entries();
                assertThat(latencyTrackingStateMetric.getEntriesInitCount())
                        .isEqualTo(expectedResult);

                latencyTrackingState.keys();
                assertThat(latencyTrackingStateMetric.getKeysInitCount()).isEqualTo(expectedResult);

                latencyTrackingState.values();
                assertThat(latencyTrackingStateMetric.getValuesInitCount())
                        .isEqualTo(expectedResult);

                latencyTrackingState.iterator();
                assertThat(latencyTrackingStateMetric.getIteratorInitCount())
                        .isEqualTo(expectedResult);
            }
        } finally {
            if (keyedBackend != null) {
                keyedBackend.close();
                keyedBackend.dispose();
            }
        }
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testLatencyTrackingMapStateIterator() throws Exception {
        AbstractKeyedStateBackend<Integer> keyedBackend = createKeyedBackend(getKeySerializer());
        try {
            LatencyTrackingMapState<Integer, VoidNamespace, Long, Double> latencyTrackingState =
                    (LatencyTrackingMapState)
                            createLatencyTrackingState(keyedBackend, getStateDescriptor());
            latencyTrackingState.setCurrentNamespace(VoidNamespace.INSTANCE);
            LatencyTrackingMapState.MapStateLatencyMetrics latencyTrackingStateMetric =
                    latencyTrackingState.getLatencyTrackingStateMetric();

            setCurrentKey(keyedBackend);

            verifyIterator(
                    latencyTrackingState,
                    latencyTrackingStateMetric,
                    latencyTrackingState.iterator(),
                    true);
            verifyIterator(
                    latencyTrackingState,
                    latencyTrackingStateMetric,
                    latencyTrackingState.entries().iterator(),
                    true);
            verifyIterator(
                    latencyTrackingState,
                    latencyTrackingStateMetric,
                    latencyTrackingState.keys().iterator(),
                    false);
            verifyIterator(
                    latencyTrackingState,
                    latencyTrackingStateMetric,
                    latencyTrackingState.values().iterator(),
                    false);
        } finally {
            if (keyedBackend != null) {
                keyedBackend.close();
                keyedBackend.dispose();
            }
        }
    }

    private <E> void verifyIterator(
            LatencyTrackingMapState<Integer, VoidNamespace, Long, Double> latencyTrackingState,
            LatencyTrackingMapState.MapStateLatencyMetrics latencyTrackingStateMetric,
            Iterator<E> iterator,
            boolean removeIterator)
            throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int index = 1; index <= SAMPLE_INTERVAL; index++) {
            latencyTrackingState.put((long) index, random.nextDouble());
        }
        int count = 1;
        while (iterator.hasNext()) {
            int expectedResult = count == SAMPLE_INTERVAL ? 0 : count;
            assertThat(latencyTrackingStateMetric.getIteratorHasNextCount())
                    .isEqualTo(expectedResult);

            iterator.next();
            assertThat(latencyTrackingStateMetric.getIteratorNextCount()).isEqualTo(expectedResult);

            if (removeIterator) {
                iterator.remove();
                assertThat(latencyTrackingStateMetric.getIteratorRemoveCount())
                        .isEqualTo(expectedResult);
            }
            count += 1;
        }
        // as we call #hasNext on more time than #next, to avoid complex check, just reset hasNext
        // counter in the end.
        latencyTrackingStateMetric.resetIteratorHasNextCount();
        latencyTrackingState.clear();
    }
}
