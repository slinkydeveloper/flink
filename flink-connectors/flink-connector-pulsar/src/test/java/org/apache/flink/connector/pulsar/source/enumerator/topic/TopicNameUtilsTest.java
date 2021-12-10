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

package org.apache.flink.connector.pulsar.source.enumerator.topic;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link TopicNameUtils}. */
class TopicNameUtilsTest {

    private static final String fullTopicName = "persistent://tenant/cluster/namespace/topic";
    private static final String topicNameWithLocal =
            "persistent://tenant/cluster/namespace/ns-abc/table/1";
    private static final String shortTopicName = "short-topic";
    private static final String topicNameWithoutCluster = "persistent://tenant/namespace/topic";

    @Test
    void topicNameWouldReturnACleanTopicNameWithTenant() {
        String name1 = TopicNameUtils.topicName(fullTopicName + "-partition-1");
        assertThat(fullTopicName).isEqualTo(name1);

        String name2 = TopicNameUtils.topicName(topicNameWithLocal);
        assertThat(topicNameWithLocal).isEqualTo(name2);

        String name3 = TopicNameUtils.topicName(shortTopicName + "-partition-1");
        assertThat("persistent://public/default/short-topic").isEqualTo(name3);

        String name4 = TopicNameUtils.topicName(shortTopicName);
        assertThat("persistent://public/default/short-topic").isEqualTo(name4);

        String name5 = TopicNameUtils.topicName(topicNameWithoutCluster + "-partition-1");
        assertThat(topicNameWithoutCluster).isEqualTo(name5);
    }

    @Test
    void topicNameWithPartitionInfo() {
        assertThatThrownBy(() -> TopicNameUtils.topicNameWithPartition(shortTopicName, -3))
                .isInstanceOf(IllegalArgumentException.class);

        String name1 = TopicNameUtils.topicNameWithPartition(fullTopicName, 4);
        assertThat(fullTopicName + "-partition-4").isEqualTo(name1);

        String name2 = TopicNameUtils.topicNameWithPartition(topicNameWithLocal, 3);
        assertThat(topicNameWithLocal + "-partition-3").isEqualTo(name2);

        String name3 = TopicNameUtils.topicNameWithPartition(shortTopicName, 5);
        assertThat("persistent://public/default/short-topic-partition-5").isEqualTo(name3);

        String name4 = TopicNameUtils.topicNameWithPartition(topicNameWithoutCluster, 8);
        assertThat(topicNameWithoutCluster + "-partition-8").isEqualTo(name4);
    }
}
