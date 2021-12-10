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
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.HamcrestCondition.matching;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.core.IsNull.notNullValue;

/** Tests for {@link NetworkBufferPool}. */
public class NetworkBufferPoolTest extends TestLogger {

    @Rule public ExpectedException expectedException = ExpectedException.none();

    @Rule public Timeout timeout = new Timeout(10, TimeUnit.SECONDS);

    @Test
    public void testCreatePoolAfterDestroy() {
        try {
            final int bufferSize = 128;
            final int numBuffers = 10;

            NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, bufferSize);
            assertThat(globalPool.getNumberOfRegisteredBufferPools()).isEqualTo(0);

            globalPool.destroy();

            assertThat(globalPool.isDestroyed()).isTrue();

            try {
                globalPool.createBufferPool(2, 2);
                fail("Should throw an IllegalStateException");
            } catch (IllegalStateException e) {
                // yippie!
            }

            try {
                globalPool.createBufferPool(2, 10);
                fail("Should throw an IllegalStateException");
            } catch (IllegalStateException e) {
                // yippie!
            }

            try {
                globalPool.createBufferPool(2, Integer.MAX_VALUE);
                fail("Should throw an IllegalStateException");
            } catch (IllegalStateException e) {
                // yippie!
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testMemoryUsageInTheContextOfMemoryPoolCreation() {
        final int bufferSize = 128;
        final int numBuffers = 10;

        NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, bufferSize);

        assertThat(globalPool.getTotalNumberOfMemorySegments()).isEqualTo(numBuffers);
        assertThat(globalPool.getNumberOfAvailableMemorySegments()).isEqualTo(numBuffers);
        assertThat(globalPool.getNumberOfUsedMemorySegments()).isEqualTo(0);

        assertThat(globalPool.getTotalMemory()).isEqualTo((long) numBuffers * bufferSize);
        assertThat(globalPool.getAvailableMemory()).isEqualTo((long) numBuffers * bufferSize);
        assertThat(globalPool.getUsedMemory()).isEqualTo(0L);
    }

    @Test
    public void testMemoryUsageInTheContextOfMemorySegmentAllocation() {
        final int bufferSize = 128;
        final int numBuffers = 10;

        NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, bufferSize);

        MemorySegment segment = globalPool.requestMemorySegment();
        assertThat(segment).isEqualTo(notNullValue());

        assertThat(globalPool.getTotalNumberOfMemorySegments()).isEqualTo(numBuffers);
        assertThat(globalPool.getNumberOfAvailableMemorySegments()).isEqualTo(numBuffers - 1);
        assertThat(globalPool.getNumberOfUsedMemorySegments()).isEqualTo(1);

        assertThat(globalPool.getTotalMemory()).isEqualTo((long) numBuffers * bufferSize);
        assertThat(globalPool.getAvailableMemory()).isEqualTo((long) (numBuffers - 1) * bufferSize);
        assertThat(globalPool.getUsedMemory()).isEqualTo((long) bufferSize);
    }

    @Test
    public void testMemoryUsageInTheContextOfMemoryPoolDestruction() {
        final int bufferSize = 128;
        final int numBuffers = 10;

        NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, bufferSize);

        globalPool.destroy();

        assertThat(globalPool.getTotalNumberOfMemorySegments()).isEqualTo(0);
        assertThat(globalPool.getNumberOfAvailableMemorySegments()).isEqualTo(0);
        assertThat(globalPool.getNumberOfUsedMemorySegments()).isEqualTo(0);

        assertThat(globalPool.getTotalMemory()).isEqualTo(0L);
        assertThat(globalPool.getAvailableMemory()).isEqualTo(0L);
        assertThat(globalPool.getUsedMemory()).isEqualTo(0L);
    }

    @Test
    public void testDestroyAll() throws IOException {
        NetworkBufferPool globalPool = new NetworkBufferPool(10, 128);

        BufferPool fixedPool = globalPool.createBufferPool(2, 2);
        BufferPool boundedPool = globalPool.createBufferPool(1, 1);
        BufferPool nonFixedPool = globalPool.createBufferPool(5, Integer.MAX_VALUE);

        assertThat(fixedPool.getNumberOfRequiredMemorySegments()).isEqualTo(2);
        assertThat(boundedPool.getNumberOfRequiredMemorySegments()).isEqualTo(1);
        assertThat(nonFixedPool.getNumberOfRequiredMemorySegments()).isEqualTo(5);

        // actually, the buffer pool sizes may be different due to rounding and based on the
        // internal order of
        // the buffer pools - the total number of retrievable buffers should be equal to the number
        // of buffers
        // in the NetworkBufferPool though

        ArrayList<Buffer> buffers = new ArrayList<>(globalPool.getTotalNumberOfMemorySegments());
        collectBuffers:
        for (int i = 0; i < 10; ++i) {
            for (BufferPool bp : new BufferPool[] {fixedPool, boundedPool, nonFixedPool}) {
                Buffer buffer = bp.requestBuffer();
                if (buffer != null) {
                    assertThat(buffer.getMemorySegment()).isNotNull();
                    buffers.add(buffer);
                    continue collectBuffers;
                }
            }
        }

        assertThat(buffers.size()).isEqualTo(globalPool.getTotalNumberOfMemorySegments());

        assertThat(fixedPool.requestBuffer()).isNull();
        assertThat(boundedPool.requestBuffer()).isNull();
        assertThat(nonFixedPool.requestBuffer()).isNull();

        // destroy all allocated ones
        globalPool.destroyAllBufferPools();

        // check the destroyed status
        assertThat(globalPool.isDestroyed()).isFalse();
        assertThat(fixedPool.isDestroyed()).isTrue();
        assertThat(boundedPool.isDestroyed()).isTrue();
        assertThat(nonFixedPool.isDestroyed()).isTrue();

        assertThat(globalPool.getNumberOfRegisteredBufferPools()).isEqualTo(0);

        // buffers are not yet recycled
        assertThat(globalPool.getNumberOfAvailableMemorySegments()).isEqualTo(0);

        // the recycled buffers should go to the global pool
        for (Buffer b : buffers) {
            b.recycleBuffer();
        }
        assertThat(globalPool.getNumberOfAvailableMemorySegments())
                .isEqualTo(globalPool.getTotalNumberOfMemorySegments());

        // can request no more buffers
        try {
            fixedPool.requestBuffer();
            fail("Should fail with an CancelTaskException");
        } catch (CancelTaskException e) {
            // yippie!
        }

        try {
            boundedPool.requestBuffer();
            fail("Should fail with an CancelTaskException");
        } catch (CancelTaskException e) {
            // that's the way we like it, aha, aha
        }

        try {
            nonFixedPool.requestBuffer();
            fail("Should fail with an CancelTaskException");
        } catch (CancelTaskException e) {
            // stayin' alive
        }

        // can create a new pool now
        assertThat(globalPool.createBufferPool(10, Integer.MAX_VALUE)).isNotNull();
    }

    /**
     * Tests {@link NetworkBufferPool#requestMemorySegments(int)} with the {@link NetworkBufferPool}
     * currently containing the number of required free segments.
     */
    @Test
    public void testRequestMemorySegmentsLessThanTotalBuffers() throws IOException {
        final int numBuffers = 10;

        NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128);

        List<MemorySegment> memorySegments = Collections.emptyList();
        try {
            memorySegments = globalPool.requestMemorySegments(numBuffers / 2);
            assertThat(numBuffers / 2).isEqualTo(memorySegments.size());

            globalPool.recycleMemorySegments(memorySegments);
            memorySegments.clear();
            assertThat(numBuffers).isEqualTo(globalPool.getNumberOfAvailableMemorySegments());
        } finally {
            globalPool.recycleMemorySegments(memorySegments); // just in case
            globalPool.destroy();
        }
    }

    /**
     * Tests {@link NetworkBufferPool#requestMemorySegments(int)} with the number of required
     * buffers exceeding the capacity of {@link NetworkBufferPool}.
     */
    @Test
    public void testRequestMemorySegmentsMoreThanTotalBuffers() {
        final int numBuffers = 10;

        NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128);

        try {
            globalPool.requestMemorySegments(numBuffers + 1);
            fail("Should throw an IOException");
        } catch (IOException e) {
            assertThat(numBuffers).isEqualTo(globalPool.getNumberOfAvailableMemorySegments());
        } finally {
            globalPool.destroy();
        }
    }

    /**
     * Tests {@link NetworkBufferPool#requestMemorySegments(int)} with the invalid argument to cause
     * exception.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testRequestMemorySegmentsWithInvalidArgument() throws IOException {
        NetworkBufferPool globalPool = new NetworkBufferPool(10, 128);
        // the number of requested buffers should be non-negative
        globalPool.requestMemorySegments(-1);
        globalPool.destroy();
        fail("Should throw an IllegalArgumentException");
    }

    /**
     * Tests {@link NetworkBufferPool#requestMemorySegments(int)} with the {@link NetworkBufferPool}
     * currently not containing the number of required free segments (currently occupied by a buffer
     * pool).
     */
    @Test
    public void testRequestMemorySegmentsWithBuffersTaken()
            throws IOException, InterruptedException {
        final int numBuffers = 10;

        NetworkBufferPool networkBufferPool = new NetworkBufferPool(numBuffers, 128);

        final List<Buffer> buffers = new ArrayList<>(numBuffers);
        List<MemorySegment> memorySegments = Collections.emptyList();
        Thread bufferRecycler = null;
        BufferPool lbp1 = null;
        try {
            lbp1 = networkBufferPool.createBufferPool(numBuffers / 2, numBuffers);

            // take all buffers (more than the minimum required)
            for (int i = 0; i < numBuffers; ++i) {
                Buffer buffer = lbp1.requestBuffer();
                buffers.add(buffer);
                assertThat(buffer).isNotNull();
            }

            // requestMemorySegments() below will wait for buffers
            // this will make sure that enough buffers are freed eventually for it to continue
            final OneShotLatch isRunning = new OneShotLatch();
            bufferRecycler =
                    new Thread(
                            () -> {
                                try {
                                    isRunning.trigger();
                                    Thread.sleep(100);
                                } catch (InterruptedException ignored) {
                                }

                                for (Buffer buffer : buffers) {
                                    buffer.recycleBuffer();
                                }
                            });
            bufferRecycler.start();

            // take more buffers than are freely available at the moment via requestMemorySegments()
            isRunning.await();
            memorySegments = networkBufferPool.requestMemorySegments(numBuffers / 2);
            assertThat(memorySegments).doesNotContainNull();
        } finally {
            if (bufferRecycler != null) {
                bufferRecycler.join();
            }
            if (lbp1 != null) {
                lbp1.lazyDestroy();
            }
            networkBufferPool.recycleMemorySegments(memorySegments);
            networkBufferPool.destroy();
        }
    }

    /**
     * Tests {@link NetworkBufferPool#requestMemorySegments(int)}, verifying it may be aborted in
     * case of a concurrent {@link NetworkBufferPool#destroy()} call.
     */
    @Test
    public void testRequestMemorySegmentsInterruptable() throws Exception {
        final int numBuffers = 10;

        NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128);
        MemorySegment segment = globalPool.requestMemorySegment();
        assertThat(segment).isNotNull();

        final OneShotLatch isRunning = new OneShotLatch();
        CheckedThread asyncRequest =
                new CheckedThread() {
                    @Override
                    public void go() throws IOException {
                        isRunning.trigger();
                        globalPool.requestMemorySegments(10);
                    }
                };
        asyncRequest.start();

        // We want the destroy call inside the blocking part of the
        // globalPool.requestMemorySegments()
        // call above. We cannot guarantee this though but make it highly probable:
        isRunning.await();
        Thread.sleep(10);
        globalPool.destroy();

        segment.free();

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("destroyed");
        try {
            asyncRequest.sync();
        } finally {
            globalPool.destroy();
        }
    }

    /**
     * Tests {@link NetworkBufferPool#requestMemorySegments(int)}, verifying it may be aborted and
     * remains in a defined state even if the waiting is interrupted.
     */
    @Test
    public void testRequestMemorySegmentsInterruptable2() throws Exception {
        final int numBuffers = 10;

        NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128);
        MemorySegment segment = globalPool.requestMemorySegment();
        assertThat(segment).isNotNull();

        final OneShotLatch isRunning = new OneShotLatch();
        CheckedThread asyncRequest =
                new CheckedThread() {
                    @Override
                    public void go() throws IOException {
                        isRunning.trigger();
                        globalPool.requestMemorySegments(10);
                    }
                };
        asyncRequest.start();

        // We want the destroy call inside the blocking part of the
        // globalPool.requestMemorySegments()
        // call above. We cannot guarantee this though but make it highly probable:
        isRunning.await();
        Thread.sleep(10);
        asyncRequest.interrupt();

        globalPool.recycle(segment);

        try {
            asyncRequest.sync();
        } catch (IOException e) {
            assertThat(e)
                    .satisfies(
                            matching(hasProperty("cause", instanceOf(InterruptedException.class))));

            // test indirectly for NetworkBufferPool#numTotalRequiredBuffers being correct:
            // -> creating a new buffer pool should not fail
            globalPool.createBufferPool(10, 10);
        } finally {
            globalPool.destroy();
        }
    }

    /**
     * Tests {@link NetworkBufferPool#requestMemorySegments(int)} and verifies it will end
     * exceptionally when failing to acquire all the segments in the specific timeout.
     */
    @Test
    public void testRequestMemorySegmentsTimeout() throws Exception {
        final int numBuffers = 10;
        final int numberOfSegmentsToRequest = 2;
        final Duration requestSegmentsTimeout = Duration.ofMillis(50L);

        NetworkBufferPool globalPool =
                new NetworkBufferPool(numBuffers, 128, requestSegmentsTimeout);

        BufferPool localBufferPool = globalPool.createBufferPool(1, numBuffers);
        for (int i = 0; i < numBuffers; ++i) {
            localBufferPool.requestBuffer();
        }

        assertThat(globalPool.getNumberOfAvailableMemorySegments()).isEqualTo(0);

        CheckedThread asyncRequest =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        globalPool.requestMemorySegments(numberOfSegmentsToRequest);
                    }
                };

        asyncRequest.start();

        expectedException.expect(IOException.class);
        expectedException.expectMessage("Timeout");

        try {
            asyncRequest.sync();
        } finally {
            globalPool.destroy();
        }
    }

    /**
     * Tests {@link NetworkBufferPool#isAvailable()}, verifying that the buffer availability is
     * correctly maintained after memory segments are requested by {@link
     * NetworkBufferPool#requestMemorySegment()} and recycled by {@link
     * NetworkBufferPool#recycle(MemorySegment)}.
     */
    @Test
    public void testIsAvailableOrNotAfterRequestAndRecycleSingleSegment() {
        final int numBuffers = 2;

        final NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128);

        try {
            // the global pool should be in available state initially
            assertThat(globalPool.getAvailableFuture().isDone()).isTrue();

            // request the first segment
            final MemorySegment segment1 = checkNotNull(globalPool.requestMemorySegment());
            assertThat(globalPool.getAvailableFuture().isDone()).isTrue();

            // request the second segment
            final MemorySegment segment2 = checkNotNull(globalPool.requestMemorySegment());
            assertThat(globalPool.getAvailableFuture().isDone()).isFalse();

            final CompletableFuture<?> availableFuture = globalPool.getAvailableFuture();

            // recycle the first segment
            globalPool.recycle(segment1);
            assertThat(availableFuture.isDone()).isTrue();
            assertThat(globalPool.getAvailableFuture().isDone()).isTrue();

            // recycle the second segment
            globalPool.recycle(segment2);
            assertThat(globalPool.getAvailableFuture().isDone()).isTrue();

        } finally {
            globalPool.destroy();
        }
    }

    /**
     * Tests {@link NetworkBufferPool#isAvailable()}, verifying that the buffer availability is
     * correctly maintained after memory segments are requested by {@link
     * NetworkBufferPool#requestMemorySegments(int)} and recycled by {@link
     * NetworkBufferPool#recycleMemorySegments(Collection)}.
     */
    @Test(timeout = 10000L)
    public void testIsAvailableOrNotAfterRequestAndRecycleMultiSegments()
            throws InterruptedException, IOException {
        final int numberOfSegmentsToRequest = 5;
        final int numBuffers = 2 * numberOfSegmentsToRequest;

        final NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128);

        try {
            // the global pool should be in available state initially
            assertThat(globalPool.getAvailableFuture().isDone()).isTrue();

            // request 5 segments
            List<MemorySegment> segments1 =
                    globalPool.requestMemorySegments(numberOfSegmentsToRequest);
            assertThat(globalPool.getAvailableFuture().isDone()).isTrue();
            assertThat(segments1.size()).isEqualTo(numberOfSegmentsToRequest);

            // request another 5 segments
            List<MemorySegment> segments2 =
                    globalPool.requestMemorySegments(numberOfSegmentsToRequest);
            assertThat(globalPool.getAvailableFuture().isDone()).isFalse();
            assertThat(segments2.size()).isEqualTo(numberOfSegmentsToRequest);

            // request another 5 segments
            final CountDownLatch latch = new CountDownLatch(1);
            final List<MemorySegment> segments3 = new ArrayList<>(numberOfSegmentsToRequest);
            CheckedThread asyncRequest =
                    new CheckedThread() {
                        @Override
                        public void go() throws Exception {
                            // this request should be blocked until at least 5 segments are recycled
                            segments3.addAll(
                                    globalPool.requestMemorySegments(numberOfSegmentsToRequest));
                            latch.countDown();
                        }
                    };
            asyncRequest.start();

            // recycle 5 segments
            CompletableFuture<?> availableFuture = globalPool.getAvailableFuture();
            globalPool.recycleMemorySegments(segments1);
            assertThat(availableFuture.isDone()).isTrue();

            // wait util the third request is fulfilled
            latch.await();
            assertThat(globalPool.getAvailableFuture().isDone()).isFalse();
            assertThat(segments3.size()).isEqualTo(numberOfSegmentsToRequest);

            // recycle another 5 segments
            globalPool.recycleMemorySegments(segments2);
            assertThat(globalPool.getAvailableFuture().isDone()).isTrue();

            // recycle the last 5 segments
            globalPool.recycleMemorySegments(segments3);
            assertThat(globalPool.getAvailableFuture().isDone()).isTrue();

        } finally {
            globalPool.destroy();
        }
    }

    /**
     * Tests that blocking request of multi local buffer pools can be fulfilled by recycled segments
     * to the global network buffer pool.
     */
    @Test
    public void testBlockingRequestFromMultiLocalBufferPool()
            throws IOException, InterruptedException {
        final int localPoolRequiredSize = 5;
        final int localPoolMaxSize = 10;
        final int numLocalBufferPool = 2;
        final int numberOfSegmentsToRequest = 10;
        final int numBuffers = numLocalBufferPool * localPoolMaxSize;

        final ExecutorService executorService = Executors.newFixedThreadPool(numLocalBufferPool);
        final NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128);
        final List<BufferPool> localBufferPools = new ArrayList<>(numLocalBufferPool);

        try {
            // create local buffer pools
            for (int i = 0; i < numLocalBufferPool; ++i) {
                final BufferPool localPool =
                        globalPool.createBufferPool(localPoolRequiredSize, localPoolMaxSize);
                localBufferPools.add(localPool);
                assertThat(localPool.getAvailableFuture().isDone()).isTrue();
            }

            // request some segments from the global pool in two different ways
            final List<MemorySegment> segments = new ArrayList<>(numberOfSegmentsToRequest - 1);
            for (int i = 0; i < numberOfSegmentsToRequest - 1; ++i) {
                segments.add(globalPool.requestMemorySegment());
            }
            final List<MemorySegment> exclusiveSegments =
                    globalPool.requestMemorySegments(
                            globalPool.getNumberOfAvailableMemorySegments() - 1);
            assertThat(globalPool.getAvailableFuture().isDone()).isTrue();
            for (final BufferPool localPool : localBufferPools) {
                assertThat(localPool.getAvailableFuture().isDone()).isTrue();
            }

            // blocking request buffers form local buffer pools
            final CountDownLatch latch = new CountDownLatch(numLocalBufferPool);
            final BlockingQueue<BufferBuilder> segmentsRequested =
                    new ArrayBlockingQueue<>(numBuffers);
            final AtomicReference<Throwable> cause = new AtomicReference<>();
            for (final BufferPool localPool : localBufferPools) {
                executorService.submit(
                        () -> {
                            try {
                                for (int num = localPoolMaxSize; num > 0; --num) {
                                    segmentsRequested.add(localPool.requestBufferBuilderBlocking());
                                }
                            } catch (Exception e) {
                                cause.set(e);
                            } finally {
                                latch.countDown();
                            }
                        });
            }

            // wait until all available buffers are requested
            while (segmentsRequested.size() + segments.size() + exclusiveSegments.size()
                    < numBuffers) {
                Thread.sleep(100);
                assertThat(cause.get()).isNull();
            }

            final CompletableFuture<?> globalPoolAvailableFuture = globalPool.getAvailableFuture();
            assertThat(globalPoolAvailableFuture.isDone()).isFalse();

            final List<CompletableFuture<?>> localPoolAvailableFutures =
                    new ArrayList<>(numLocalBufferPool);
            for (BufferPool localPool : localBufferPools) {
                CompletableFuture<?> localPoolAvailableFuture = localPool.getAvailableFuture();
                localPoolAvailableFutures.add(localPoolAvailableFuture);
                assertThat(localPoolAvailableFuture.isDone()).isFalse();
            }

            // recycle the previously requested segments
            for (MemorySegment segment : segments) {
                globalPool.recycle(segment);
            }
            globalPool.recycleMemorySegments(exclusiveSegments);

            assertThat(globalPoolAvailableFuture.isDone()).isTrue();
            for (CompletableFuture<?> localPoolAvailableFuture : localPoolAvailableFutures) {
                assertThat(localPoolAvailableFuture.isDone()).isTrue();
            }

            // wait until all blocking buffer requests finish
            latch.await();

            assertThat(cause.get()).isNull();
            assertThat(globalPool.getNumberOfAvailableMemorySegments()).isEqualTo(0);
            assertThat(globalPool.getAvailableFuture().isDone()).isFalse();
            for (BufferPool localPool : localBufferPools) {
                assertThat(localPool.getAvailableFuture().isDone()).isFalse();
                assertThat(localPool.bestEffortGetNumOfUsedBuffers()).isEqualTo(localPoolMaxSize);
            }

            // recycle all the requested buffers
            for (BufferBuilder bufferBuilder : segmentsRequested) {
                bufferBuilder.close();
            }

        } finally {
            for (BufferPool bufferPool : localBufferPools) {
                bufferPool.lazyDestroy();
            }
            executorService.shutdown();
            globalPool.destroy();
        }
    }
}
