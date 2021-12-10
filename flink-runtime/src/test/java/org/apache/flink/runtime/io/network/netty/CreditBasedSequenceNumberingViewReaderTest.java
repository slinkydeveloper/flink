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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.partition.NoOpResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CreditBasedSequenceNumberingViewReader}. */
public class CreditBasedSequenceNumberingViewReaderTest {

    @Test
    public void testResumeConsumption() throws Exception {
        int numCredits = 2;
        CreditBasedSequenceNumberingViewReader reader1 =
                createNetworkSequenceViewReader(numCredits);

        reader1.resumeConsumption();
        assertThat(reader1.getNumCreditsAvailable()).isEqualTo(numCredits);

        reader1.addCredit(numCredits);
        reader1.resumeConsumption();
        assertThat(reader1.getNumCreditsAvailable()).isEqualTo(2 * numCredits);

        CreditBasedSequenceNumberingViewReader reader2 = createNetworkSequenceViewReader(0);

        reader2.addCredit(numCredits);
        assertThat(reader2.getNumCreditsAvailable()).isEqualTo(numCredits);

        reader2.resumeConsumption();
        assertThat(reader2.getNumCreditsAvailable()).isEqualTo(0);
    }

    @Test
    public void testNeedAnnounceBacklog() throws Exception {
        int numCredits = 2;
        CreditBasedSequenceNumberingViewReader reader1 =
                createNetworkSequenceViewReader(numCredits);

        assertThat(reader1.needAnnounceBacklog()).isFalse();
        reader1.addCredit(-numCredits);
        assertThat(reader1.needAnnounceBacklog()).isFalse();

        CreditBasedSequenceNumberingViewReader reader2 = createNetworkSequenceViewReader(0);
        assertThat(reader2.needAnnounceBacklog()).isTrue();

        reader2.addCredit(numCredits);
        assertThat(reader2.needAnnounceBacklog()).isFalse();

        reader2.addCredit(-numCredits);
        assertThat(reader2.needAnnounceBacklog()).isTrue();
    }

    private CreditBasedSequenceNumberingViewReader createNetworkSequenceViewReader(
            int initialCredit) throws Exception {
        PartitionRequestQueue queue = new PartitionRequestQueue();
        EmbeddedChannel channel = new EmbeddedChannel(queue);
        channel.close();
        CreditBasedSequenceNumberingViewReader reader =
                new CreditBasedSequenceNumberingViewReader(
                        new InputChannelID(), initialCredit, queue);
        reader.requestSubpartitionView(
                (ignored1, ignored2, ignored3) -> new NoOpResultSubpartitionView(),
                new ResultPartitionID(),
                0);
        return reader;
    }
}
