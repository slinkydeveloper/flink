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

package org.apache.flink.streaming.api.utils;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.fnexecution.v1.FlinkFnApi;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class for testing utilities used to construct protobuf objects or construct objects from
 * protobuf objects.
 */
public class ProtoUtilsTest {
    @Test
    public void testParseStateTtlConfigFromProto() {
        FlinkFnApi.StateDescriptor.StateTTLConfig.CleanupStrategies cleanupStrategiesProto =
                FlinkFnApi.StateDescriptor.StateTTLConfig.CleanupStrategies.newBuilder()
                        .setIsCleanupInBackground(true)
                        .addStrategies(
                                FlinkFnApi.StateDescriptor.StateTTLConfig.CleanupStrategies
                                        .MapStrategiesEntry.newBuilder()
                                        .setStrategy(
                                                FlinkFnApi.StateDescriptor.StateTTLConfig
                                                        .CleanupStrategies.Strategies
                                                        .FULL_STATE_SCAN_SNAPSHOT)
                                        .setEmptyStrategy(
                                                FlinkFnApi.StateDescriptor.StateTTLConfig
                                                        .CleanupStrategies.EmptyCleanupStrategy
                                                        .EMPTY_STRATEGY))
                        .addStrategies(
                                FlinkFnApi.StateDescriptor.StateTTLConfig.CleanupStrategies
                                        .MapStrategiesEntry.newBuilder()
                                        .setStrategy(
                                                FlinkFnApi.StateDescriptor.StateTTLConfig
                                                        .CleanupStrategies.Strategies
                                                        .INCREMENTAL_CLEANUP)
                                        .setIncrementalCleanupStrategy(
                                                FlinkFnApi.StateDescriptor.StateTTLConfig
                                                        .CleanupStrategies
                                                        .IncrementalCleanupStrategy.newBuilder()
                                                        .setCleanupSize(10)
                                                        .setRunCleanupForEveryRecord(true)
                                                        .build()))
                        .addStrategies(
                                FlinkFnApi.StateDescriptor.StateTTLConfig.CleanupStrategies
                                        .MapStrategiesEntry.newBuilder()
                                        .setStrategy(
                                                FlinkFnApi.StateDescriptor.StateTTLConfig
                                                        .CleanupStrategies.Strategies
                                                        .ROCKSDB_COMPACTION_FILTER)
                                        .setRocksdbCompactFilterCleanupStrategy(
                                                FlinkFnApi.StateDescriptor.StateTTLConfig
                                                        .CleanupStrategies
                                                        .RocksdbCompactFilterCleanupStrategy
                                                        .newBuilder()
                                                        .setQueryTimeAfterNumEntries(1000)
                                                        .build()))
                        .build();
        FlinkFnApi.StateDescriptor.StateTTLConfig stateTTLConfigProto =
                FlinkFnApi.StateDescriptor.StateTTLConfig.newBuilder()
                        .setTtl(Time.of(1000, TimeUnit.MILLISECONDS).toMilliseconds())
                        .setUpdateType(
                                FlinkFnApi.StateDescriptor.StateTTLConfig.UpdateType
                                        .OnCreateAndWrite)
                        .setStateVisibility(
                                FlinkFnApi.StateDescriptor.StateTTLConfig.StateVisibility
                                        .NeverReturnExpired)
                        .setCleanupStrategies(cleanupStrategiesProto)
                        .build();

        StateTtlConfig stateTTLConfig =
                ProtoUtils.parseStateTtlConfigFromProto(stateTTLConfigProto);

        assertThat(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .isEqualTo(stateTTLConfig.getUpdateType());
        assertThat(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .isEqualTo(stateTTLConfig.getStateVisibility());
        assertThat(Time.milliseconds(1000)).isEqualTo(stateTTLConfig.getTtl());
        assertThat(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime)
                .isEqualTo(stateTTLConfig.getTtlTimeCharacteristic());

        StateTtlConfig.CleanupStrategies cleanupStrategies = stateTTLConfig.getCleanupStrategies();
        assertThat(cleanupStrategies.isCleanupInBackground()).isTrue();
        assertThat(cleanupStrategies.inFullSnapshot()).isTrue();

        StateTtlConfig.IncrementalCleanupStrategy incrementalCleanupStrategy =
                cleanupStrategies.getIncrementalCleanupStrategy();
        assertThat(incrementalCleanupStrategy).isNotNull();
        assertThat(10).isEqualTo(incrementalCleanupStrategy.getCleanupSize());
        assertThat(incrementalCleanupStrategy.runCleanupForEveryRecord()).isTrue();

        StateTtlConfig.RocksdbCompactFilterCleanupStrategy rocksdbCompactFilterCleanupStrategy =
                cleanupStrategies.getRocksdbCompactFilterCleanupStrategy();
        assertThat(rocksdbCompactFilterCleanupStrategy).isNotNull();
        assertThat(1000)
                .isEqualTo(rocksdbCompactFilterCleanupStrategy.getQueryTimeAfterNumEntries());
    }
}
