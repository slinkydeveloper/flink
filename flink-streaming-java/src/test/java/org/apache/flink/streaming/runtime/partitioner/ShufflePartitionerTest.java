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

package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.api.java.tuple.Tuple;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ShufflePartitioner}. */
public class ShufflePartitionerTest extends StreamPartitionerTest {

    @Override
    public StreamPartitioner<Tuple> createPartitioner() {
        StreamPartitioner<Tuple> partitioner = new ShufflePartitioner<>();
        assertThat(partitioner.isBroadcast()).isFalse();
        return partitioner;
    }

    @Test
    public void testSelectChannelsInterval() {
        assertSelectedChannelWithSetup(0, 1);

        streamPartitioner.setup(2);
        assertThat(0 <= streamPartitioner.selectChannel(serializationDelegate)).isTrue();
        assertThat(2 > streamPartitioner.selectChannel(serializationDelegate)).isTrue();

        streamPartitioner.setup(1024);
        assertThat(0 <= streamPartitioner.selectChannel(serializationDelegate)).isTrue();
        assertThat(1024 > streamPartitioner.selectChannel(serializationDelegate)).isTrue();
    }
}
