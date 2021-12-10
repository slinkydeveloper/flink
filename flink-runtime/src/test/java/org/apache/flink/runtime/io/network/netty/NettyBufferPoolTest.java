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

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link NettyBufferPool} wrapper. */
public class NettyBufferPoolTest {

    @Test
    public void testNoHeapAllocations() throws Exception {
        NettyBufferPool nettyBufferPool = new NettyBufferPool(1);

        // Buffers should prefer to be direct
        assertThat(nettyBufferPool.buffer().isDirect()).isTrue();
        assertThat(nettyBufferPool.buffer(128).isDirect()).isTrue();
        assertThat(nettyBufferPool.buffer(128, 256).isDirect()).isTrue();

        // IO buffers should prefer to be direct
        assertThat(nettyBufferPool.ioBuffer().isDirect()).isTrue();
        assertThat(nettyBufferPool.ioBuffer(128).isDirect()).isTrue();
        assertThat(nettyBufferPool.ioBuffer(128, 256).isDirect()).isTrue();

        // Currently we fakes the heap buffer allocation with direct buffers
        assertThat(nettyBufferPool.heapBuffer().isDirect()).isTrue();
        assertThat(nettyBufferPool.heapBuffer(128).isDirect()).isTrue();
        assertThat(nettyBufferPool.heapBuffer(128, 256).isDirect()).isTrue();

        // Composite buffers allocates the corresponding type of buffers when extending its capacity
        assertThat(nettyBufferPool.compositeHeapBuffer().capacity(1024).isDirect()).isTrue();
        assertThat(nettyBufferPool.compositeHeapBuffer(10).capacity(1024).isDirect()).isTrue();

        // Is direct buffer pooled!
        assertThat(nettyBufferPool.isDirectBufferPooled()).isTrue();
    }

    @Test
    public void testAllocationsStatistics() throws Exception {
        NettyBufferPool nettyBufferPool = new NettyBufferPool(1);
        int chunkSize = nettyBufferPool.getChunkSize();

        {
            // Single large buffer allocates one chunk
            nettyBufferPool.directBuffer(chunkSize - 64);
            long allocated = nettyBufferPool.getNumberOfAllocatedBytes().get();
            assertThat(allocated).isEqualTo(chunkSize);
        }

        {
            // Allocate a little more (one more chunk required)
            nettyBufferPool.directBuffer(128);
            long allocated = nettyBufferPool.getNumberOfAllocatedBytes().get();
            assertThat(allocated).isEqualTo(2 * chunkSize);
        }
    }
}
