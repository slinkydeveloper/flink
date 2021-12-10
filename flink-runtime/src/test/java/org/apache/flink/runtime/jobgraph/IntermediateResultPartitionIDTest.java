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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.runtime.io.network.netty.NettyBufferPool;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;

/** Tests for {@link IntermediateResultPartitionID}. */
public class IntermediateResultPartitionIDTest {
    private static final NettyBufferPool ALLOCATOR = new NettyBufferPool(1);

    @Test
    public void testByteBufWriteAndRead() {
        final IntermediateResultPartitionID intermediateResultPartitionID =
                new IntermediateResultPartitionID();
        final int byteBufLen = IntermediateResultPartitionID.getByteBufLength();
        final ByteBuf byteBuf = ALLOCATOR.directBuffer(byteBufLen, byteBufLen);
        intermediateResultPartitionID.writeTo(byteBuf);

        assertThat(byteBuf.writerIndex())
                .isEqualTo(equalTo(IntermediateResultPartitionID.getByteBufLength()));
        assertThat(IntermediateResultPartitionID.fromByteBuf(byteBuf))
                .isEqualTo(equalTo(intermediateResultPartitionID));
    }
}
