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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.HamcrestCondition.matching;
import static org.hamcrest.Matchers.isOneOf;

/**
 * Tests for the creation of {@link LocalBufferPool} instances from the {@link NetworkBufferPool}
 * factory.
 */
public class BufferPoolFactoryTest {

    private static final int numBuffers = 1024;

    private static final int memorySegmentSize = 128;

    private NetworkBufferPool networkBufferPool;

    @Rule public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setupNetworkBufferPool() {
        networkBufferPool = new NetworkBufferPool(numBuffers, memorySegmentSize);
    }

    @After
    public void verifyAllBuffersReturned() {
        String msg = "Did not return all buffers to network buffer pool after test.";
        try {
            assertThat(networkBufferPool.getNumberOfAvailableMemorySegments())
                    .as(msg)
                    .isEqualTo(numBuffers);
        } finally {
            // in case buffers have actually been requested, we must release them again
            networkBufferPool.destroyAllBufferPools();
            networkBufferPool.destroy();
        }
    }

    /** Tests creating one buffer pool which requires more buffers than available. */
    @Test
    public void testRequireMoreThanPossible1() throws IOException {
        expectedException.expect(IOException.class);
        expectedException.expectMessage("Insufficient number of network buffers");

        networkBufferPool.createBufferPool(
                networkBufferPool.getTotalNumberOfMemorySegments() + 1, Integer.MAX_VALUE);
    }

    /** Tests creating two buffer pools which together require more buffers than available. */
    @Test
    public void testRequireMoreThanPossible2() throws IOException {
        expectedException.expect(IOException.class);
        expectedException.expectMessage("Insufficient number of network buffers");

        BufferPool bufferPool = null;
        try {
            bufferPool = networkBufferPool.createBufferPool(numBuffers / 2 + 1, numBuffers);
            networkBufferPool.createBufferPool(numBuffers / 2 + 1, numBuffers);
        } finally {
            if (bufferPool != null) {
                bufferPool.lazyDestroy();
            }
        }
    }

    /**
     * Tests creating two buffer pools which together require as many buffers as available but where
     * there are less buffers available to the {@link NetworkBufferPool} at the time of the second
     * {@link LocalBufferPool} creation.
     */
    @Test
    public void testOverprovisioned() throws IOException {
        // note: this is also the minimum number of buffers reserved for pool2
        int buffersToTakeFromPool1 = numBuffers / 2 + 1;
        // note: this is also the minimum number of buffers reserved for pool1
        int buffersToTakeFromPool2 = numBuffers - buffersToTakeFromPool1;

        List<Buffer> buffers = new ArrayList<>(numBuffers);
        BufferPool bufferPool1 = null, bufferPool2 = null;
        try {
            bufferPool1 = networkBufferPool.createBufferPool(buffersToTakeFromPool2, numBuffers);

            // take more buffers than the minimum required
            for (int i = 0; i < buffersToTakeFromPool1; ++i) {
                Buffer buffer = bufferPool1.requestBuffer();
                assertThat(buffer).isNotNull();
                buffers.add(buffer);
            }
            assertThat(bufferPool1.bestEffortGetNumOfUsedBuffers())
                    .isEqualTo(buffersToTakeFromPool1);
            assertThat(bufferPool1.getNumBuffers()).isEqualTo(numBuffers);

            // create a second pool which requires more buffers than are available at the moment
            bufferPool2 = networkBufferPool.createBufferPool(buffersToTakeFromPool1, numBuffers);

            assertThat(bufferPool2.getNumBuffers())
                    .isEqualTo(bufferPool2.getNumberOfRequiredMemorySegments());
            assertThat(bufferPool1.getNumBuffers())
                    .isEqualTo(bufferPool1.getNumberOfRequiredMemorySegments());
            assertThat(bufferPool1.requestBuffer()).isNull();

            // take all remaining buffers
            for (int i = 0; i < buffersToTakeFromPool2; ++i) {
                Buffer buffer = bufferPool2.requestBuffer();
                assertThat(buffer).isNotNull();
                buffers.add(buffer);
            }
            assertThat(bufferPool2.bestEffortGetNumOfUsedBuffers())
                    .isEqualTo(buffersToTakeFromPool2);

            // we should be able to get one more but this is currently given out to bufferPool1 and
            // taken by buffer1
            assertThat(bufferPool2.requestBuffer()).isNull();

            // as soon as one excess buffer of bufferPool1 is recycled, it should be available for
            // bufferPool2
            buffers.remove(0).recycleBuffer();
            // recycle returns the excess buffer to the network buffer pool from where it's eagerly
            // fetched by pool 2
            assertThat(networkBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(0);
            // verify the number of buffers taken from the pools
            assertThat(
                            bufferPool1.bestEffortGetNumOfUsedBuffers()
                                    + bufferPool1.getNumberOfAvailableMemorySegments())
                    .isEqualTo(buffersToTakeFromPool1 - 1);
            assertThat(
                            bufferPool2.bestEffortGetNumOfUsedBuffers()
                                    + bufferPool2.getNumberOfAvailableMemorySegments())
                    .isEqualTo(buffersToTakeFromPool2 + 1);
        } finally {
            for (Buffer buffer : buffers) {
                buffer.recycleBuffer();
            }
            if (bufferPool1 != null) {
                bufferPool1.lazyDestroy();
            }
            if (bufferPool2 != null) {
                bufferPool2.lazyDestroy();
            }
        }
    }

    @Test
    public void testBoundedPools() throws IOException {
        BufferPool bufferPool1 = networkBufferPool.createBufferPool(1, 1);
        assertThat(bufferPool1.getNumBuffers()).isEqualTo(1);

        BufferPool bufferPool2 = networkBufferPool.createBufferPool(1, 2);
        assertThat(bufferPool2.getNumBuffers()).isEqualTo(2);

        bufferPool1.lazyDestroy();
        bufferPool2.lazyDestroy();
    }

    @Test
    public void testSingleManagedPoolGetsAll() throws IOException {
        BufferPool bufferPool = networkBufferPool.createBufferPool(1, Integer.MAX_VALUE);

        assertThat(bufferPool.getNumBuffers())
                .isEqualTo(networkBufferPool.getTotalNumberOfMemorySegments());

        bufferPool.lazyDestroy();
    }

    @Test
    public void testSingleManagedPoolGetsAllExceptFixedOnes() throws IOException {
        BufferPool fixedBufferPool = networkBufferPool.createBufferPool(24, 24);

        BufferPool flexibleBufferPool = networkBufferPool.createBufferPool(1, Integer.MAX_VALUE);

        assertThat(fixedBufferPool.getNumBuffers()).isEqualTo(24);
        assertThat(flexibleBufferPool.getNumBuffers())
                .isEqualTo(
                        networkBufferPool.getTotalNumberOfMemorySegments()
                                - fixedBufferPool.getNumBuffers());

        fixedBufferPool.lazyDestroy();
        flexibleBufferPool.lazyDestroy();
    }

    @Test
    public void testUniformDistribution() throws IOException {
        BufferPool first = networkBufferPool.createBufferPool(1, Integer.MAX_VALUE);
        assertThat(first.getNumBuffers())
                .isEqualTo(networkBufferPool.getTotalNumberOfMemorySegments());

        BufferPool second = networkBufferPool.createBufferPool(1, Integer.MAX_VALUE);
        assertThat(first.getNumBuffers())
                .isEqualTo(networkBufferPool.getTotalNumberOfMemorySegments() / 2);
        assertThat(second.getNumBuffers())
                .isEqualTo(networkBufferPool.getTotalNumberOfMemorySegments() / 2);

        first.lazyDestroy();
        second.lazyDestroy();
    }

    /**
     * Tests that buffers, once given to an initial buffer pool, get re-distributed to a second one
     * in case both buffer pools request half of the available buffer count.
     */
    @Test
    public void testUniformDistributionAllBuffers() throws IOException {
        BufferPool first =
                networkBufferPool.createBufferPool(
                        networkBufferPool.getTotalNumberOfMemorySegments() / 2, Integer.MAX_VALUE);
        assertThat(first.getNumBuffers())
                .isEqualTo(networkBufferPool.getTotalNumberOfMemorySegments());

        BufferPool second =
                networkBufferPool.createBufferPool(
                        networkBufferPool.getTotalNumberOfMemorySegments() / 2, Integer.MAX_VALUE);
        assertThat(first.getNumBuffers())
                .isEqualTo(networkBufferPool.getTotalNumberOfMemorySegments() / 2);
        assertThat(second.getNumBuffers())
                .isEqualTo(networkBufferPool.getTotalNumberOfMemorySegments() / 2);

        first.lazyDestroy();
        second.lazyDestroy();
    }

    @Test
    public void testUniformDistributionBounded1() throws IOException {
        BufferPool first =
                networkBufferPool.createBufferPool(
                        1, networkBufferPool.getTotalNumberOfMemorySegments());
        assertThat(first.getNumBuffers())
                .isEqualTo(networkBufferPool.getTotalNumberOfMemorySegments());

        BufferPool second =
                networkBufferPool.createBufferPool(
                        1, networkBufferPool.getTotalNumberOfMemorySegments());
        assertThat(first.getNumBuffers())
                .isEqualTo(networkBufferPool.getTotalNumberOfMemorySegments() / 2);
        assertThat(second.getNumBuffers())
                .isEqualTo(networkBufferPool.getTotalNumberOfMemorySegments() / 2);

        first.lazyDestroy();
        second.lazyDestroy();
    }

    @Test
    public void testUniformDistributionBounded2() throws IOException {
        BufferPool first = networkBufferPool.createBufferPool(1, 10);
        assertThat(first.getNumBuffers()).isEqualTo(10);

        BufferPool second = networkBufferPool.createBufferPool(1, 10);
        assertThat(first.getNumBuffers()).isEqualTo(10);
        assertThat(second.getNumBuffers()).isEqualTo(10);

        first.lazyDestroy();
        second.lazyDestroy();
    }

    @Test
    public void testUniformDistributionBounded3() throws IOException {
        NetworkBufferPool globalPool = new NetworkBufferPool(3, 128);
        try {
            BufferPool first = globalPool.createBufferPool(1, 10);
            assertThat(first.getNumBuffers()).isEqualTo(3);

            BufferPool second = globalPool.createBufferPool(1, 10);
            // the order of which buffer pool received 2 or 1 buffer is undefined
            assertThat(first.getNumBuffers() + second.getNumBuffers()).isEqualTo(3);
            assertThat(first.getNumBuffers()).isEqualTo(3);
            assertThat(second.getNumBuffers()).isEqualTo(3);

            BufferPool third = globalPool.createBufferPool(1, 10);
            assertThat(first.getNumBuffers()).isEqualTo(1);
            assertThat(second.getNumBuffers()).isEqualTo(1);
            assertThat(third.getNumBuffers()).isEqualTo(1);

            // similar to #verifyAllBuffersReturned()
            String msg = "Wrong number of available segments after creating buffer pools.";
            assertThat(globalPool.getNumberOfAvailableMemorySegments()).as(msg).isEqualTo(0);
        } finally {
            // in case buffers have actually been requested, we must release them again
            globalPool.destroyAllBufferPools();
            globalPool.destroy();
        }
    }

    /**
     * Tests the interaction of requesting memory segments and creating local buffer pool and
     * verifies the number of assigned buffers match after redistributing buffers because of newly
     * requested memory segments or new buffer pools created.
     */
    @Test
    public void testUniformDistributionBounded4() throws IOException {
        NetworkBufferPool globalPool = new NetworkBufferPool(10, 128);
        try {
            BufferPool first = globalPool.createBufferPool(1, 10);
            assertThat(first.getNumBuffers()).isEqualTo(10);

            List<MemorySegment> segmentList1 = globalPool.requestMemorySegments(2);
            assertThat(segmentList1.size()).isEqualTo(2);
            assertThat(first.getNumBuffers()).isEqualTo(8);

            BufferPool second = globalPool.createBufferPool(1, 10);
            assertThat(first.getNumBuffers()).isEqualTo(4);
            assertThat(second.getNumBuffers()).isEqualTo(4);

            List<MemorySegment> segmentList2 = globalPool.requestMemorySegments(2);
            assertThat(segmentList2.size()).isEqualTo(2);
            assertThat(first.getNumBuffers()).isEqualTo(3);
            assertThat(second.getNumBuffers()).isEqualTo(3);

            List<MemorySegment> segmentList3 = globalPool.requestMemorySegments(2);
            assertThat(segmentList3.size()).isEqualTo(2);
            assertThat(first.getNumBuffers()).isEqualTo(2);
            assertThat(second.getNumBuffers()).isEqualTo(2);

            String msg =
                    "Wrong number of available segments after creating buffer pools and requesting segments.";
            assertThat(globalPool.getNumberOfAvailableMemorySegments()).as(msg).isEqualTo(2);

            globalPool.recycleMemorySegments(segmentList1);
            assertThat(globalPool.getNumberOfAvailableMemorySegments()).as(msg).isEqualTo(4);
            assertThat(first.getNumBuffers()).isEqualTo(3);
            assertThat(second.getNumBuffers()).isEqualTo(3);

            globalPool.recycleMemorySegments(segmentList2);
            assertThat(globalPool.getNumberOfAvailableMemorySegments()).as(msg).isEqualTo(6);
            assertThat(first.getNumBuffers()).isEqualTo(4);
            assertThat(second.getNumBuffers()).isEqualTo(4);

            globalPool.recycleMemorySegments(segmentList3);
            assertThat(globalPool.getNumberOfAvailableMemorySegments()).as(msg).isEqualTo(8);
            assertThat(first.getNumBuffers()).isEqualTo(5);
            assertThat(second.getNumBuffers()).isEqualTo(5);

            first.lazyDestroy();
            assertThat(globalPool.getNumberOfAvailableMemorySegments()).as(msg).isEqualTo(9);
            assertThat(second.getNumBuffers()).isEqualTo(10);
        } finally {
            globalPool.destroyAllBufferPools();
            globalPool.destroy();
        }
    }

    @Test
    public void testBufferRedistributionMixed1() throws IOException {
        // try multiple times for various orders during redistribution
        for (int i = 0; i < 1_000; ++i) {
            BufferPool first = networkBufferPool.createBufferPool(1, 10);
            assertThat(first.getNumBuffers()).isEqualTo(10);

            BufferPool second = networkBufferPool.createBufferPool(1, 10);
            assertThat(first.getNumBuffers()).isEqualTo(10);
            assertThat(second.getNumBuffers()).isEqualTo(10);

            BufferPool third = networkBufferPool.createBufferPool(1, Integer.MAX_VALUE);
            // note: exact buffer distribution depends on the order during the redistribution
            for (BufferPool bp : new BufferPool[] {first, second, third}) {
                final int avail = numBuffers - 3;
                int size =
                        avail
                                        * Math.min(avail, bp.getMaxNumberOfMemorySegments() - 1)
                                        / (avail + 20 - 2)
                                + 1;
                assertThat(bp.getNumBuffers())
                        .as("Wrong buffer pool size after redistribution")
                        .satisfies(matching(isOneOf(size, size + 1)));
            }

            BufferPool fourth = networkBufferPool.createBufferPool(1, Integer.MAX_VALUE);
            // note: exact buffer distribution depends on the order during the redistribution
            for (BufferPool bp : new BufferPool[] {first, second, third, fourth}) {
                final int avail = numBuffers - 4;
                int size =
                        avail
                                        * Math.min(avail, bp.getMaxNumberOfMemorySegments() - 1)
                                        / (2 * avail + 20 - 2)
                                + 1;
                assertThat(bp.getNumBuffers())
                        .as("Wrong buffer pool size after redistribution")
                        .satisfies(matching(isOneOf(size, size + 1)));
            }

            Stream.of(first, second, third, fourth).forEach(BufferPool::lazyDestroy);
            verifyAllBuffersReturned();
            setupNetworkBufferPool();
        }
    }

    @Test
    public void testAllDistributed() throws IOException {
        // try multiple times for various orders during redistribution
        for (int i = 0; i < 1_000; ++i) {
            Random random = new Random();

            List<BufferPool> pools = new ArrayList<>();

            int numPools = numBuffers / 32;
            long maxTotalUsed = 0;
            for (int j = 0; j < numPools; j++) {
                int numRequiredBuffers = random.nextInt(7) + 1;
                // make unbounded buffers more likely:
                int maxUsedBuffers =
                        random.nextBoolean()
                                ? Integer.MAX_VALUE
                                : Math.max(1, random.nextInt(10) + numRequiredBuffers);
                pools.add(networkBufferPool.createBufferPool(numRequiredBuffers, maxUsedBuffers));
                maxTotalUsed = Math.min(numBuffers, maxTotalUsed + maxUsedBuffers);

                // after every iteration, all buffers (up to maxTotalUsed) must be distributed
                int numDistributedBuffers = 0;
                for (BufferPool pool : pools) {
                    numDistributedBuffers += pool.getNumBuffers();
                }
                assertThat(numDistributedBuffers).isEqualTo(maxTotalUsed);
            }

            pools.forEach(BufferPool::lazyDestroy);
            verifyAllBuffersReturned();
            setupNetworkBufferPool();
        }
    }

    @Test
    public void testCreateDestroy() throws IOException {
        BufferPool first = networkBufferPool.createBufferPool(1, Integer.MAX_VALUE);

        assertThat(first.getNumBuffers())
                .isEqualTo(networkBufferPool.getTotalNumberOfMemorySegments());

        BufferPool second = networkBufferPool.createBufferPool(1, Integer.MAX_VALUE);

        assertThat(first.getNumBuffers())
                .isEqualTo(networkBufferPool.getTotalNumberOfMemorySegments() / 2);

        assertThat(second.getNumBuffers())
                .isEqualTo(networkBufferPool.getTotalNumberOfMemorySegments() / 2);

        first.lazyDestroy();

        assertThat(second.getNumBuffers())
                .isEqualTo(networkBufferPool.getTotalNumberOfMemorySegments());

        second.lazyDestroy();
    }
}
