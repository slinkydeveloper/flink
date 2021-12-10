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
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Tests for the {@link LocalBufferPool}. */
public class LocalBufferPoolTest extends TestLogger {

    private static final int numBuffers = 1024;

    private static final int memorySegmentSize = 128;

    private NetworkBufferPool networkBufferPool;

    private BufferPool localBufferPool;

    private static final ExecutorService executor = Executors.newCachedThreadPool();

    @Before
    public void setupLocalBufferPool() throws Exception {
        networkBufferPool = new NetworkBufferPool(numBuffers, memorySegmentSize);
        localBufferPool = new LocalBufferPool(networkBufferPool, 1);

        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(1);
    }

    @After
    public void destroyAndVerifyAllBuffersReturned() {
        if (!localBufferPool.isDestroyed()) {
            localBufferPool.lazyDestroy();
        }

        String msg = "Did not return all buffers to memory segment pool after test.";
        assertThat(networkBufferPool.getNumberOfAvailableMemorySegments())
                .as(msg)
                .isEqualTo(numBuffers);
        // no other local buffer pools used than the one above, but call just in case
        networkBufferPool.destroyAllBufferPools();
        networkBufferPool.destroy();
    }

    @AfterClass
    public static void shutdownExecutor() {
        executor.shutdownNow();
    }

    @Test
    public void testReserveSegments() throws Exception {
        NetworkBufferPool networkBufferPool =
                new NetworkBufferPool(2, memorySegmentSize, Duration.ofSeconds(2));
        try {
            BufferPool bufferPool1 = networkBufferPool.createBufferPool(1, 2);
            assertThatThrownBy(() -> bufferPool1.reserveSegments(2))
                    .isInstanceOf(IllegalArgumentException.class);

            // request all buffers
            ArrayList<Buffer> buffers = new ArrayList<>(2);
            buffers.add(bufferPool1.requestBuffer());
            buffers.add(bufferPool1.requestBuffer());
            assertThat(buffers.size()).isEqualTo(2);

            BufferPool bufferPool2 = networkBufferPool.createBufferPool(1, 10);
            assertThatThrownBy(() -> bufferPool2.reserveSegments(1))
                    .isInstanceOf(IOException.class);
            assertThat(bufferPool2.isAvailable()).isFalse();

            buffers.forEach(Buffer::recycleBuffer);
            bufferPool1.lazyDestroy();
            bufferPool2.lazyDestroy();

            BufferPool bufferPool3 = networkBufferPool.createBufferPool(2, 10);
            assertThat(bufferPool3.getNumberOfAvailableMemorySegments()).isEqualTo(1);
            bufferPool3.reserveSegments(2);
            assertThat(bufferPool3.getNumberOfAvailableMemorySegments()).isEqualTo(2);

            bufferPool3.lazyDestroy();
            assertThatThrownBy(() -> bufferPool3.reserveSegments(1))
                    .isInstanceOf(CancelTaskException.class);
        } finally {
            networkBufferPool.destroy();
        }
    }

    @Test
    public void testRequestMoreThanAvailable() {
        localBufferPool.setNumBuffers(numBuffers);

        List<Buffer> requests = new ArrayList<Buffer>(numBuffers);

        for (int i = 1; i <= numBuffers; i++) {
            Buffer buffer = localBufferPool.requestBuffer();

            assertThat(getNumRequestedFromMemorySegmentPool())
                    .isEqualTo(Math.min(i + 1, numBuffers));
            assertThat(buffer).isNotNull();

            requests.add(buffer);
        }

        {
            // One more...
            Buffer buffer = localBufferPool.requestBuffer();
            assertThat(getNumRequestedFromMemorySegmentPool()).isEqualTo(numBuffers);
            assertThat(buffer).isNull();
        }

        for (Buffer buffer : requests) {
            buffer.recycleBuffer();
        }
    }

    @Test
    public void testSetNumAfterDestroyDoesNotProactivelyFetchSegments() {
        localBufferPool.setNumBuffers(2);
        assertThat(localBufferPool.getNumBuffers()).isEqualTo(2L);
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(1L);

        localBufferPool.lazyDestroy();
        localBufferPool.setNumBuffers(3);
        assertThat(localBufferPool.getNumBuffers()).isEqualTo(3L);
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(0L);
    }

    @Test
    public void testRecycleAfterDestroy() {
        localBufferPool.setNumBuffers(numBuffers);

        List<Buffer> requests = new ArrayList<Buffer>(numBuffers);

        for (int i = 0; i < numBuffers; i++) {
            requests.add(localBufferPool.requestBuffer());
        }

        localBufferPool.lazyDestroy();

        // All buffers have been requested, but can not be returned yet.
        assertThat(getNumRequestedFromMemorySegmentPool()).isEqualTo(numBuffers);

        // Recycle should return buffers to memory segment pool
        for (Buffer buffer : requests) {
            buffer.recycleBuffer();
        }
    }

    @Test
    public void testRecycleExcessBuffersAfterRecycling() {
        localBufferPool.setNumBuffers(numBuffers);

        List<Buffer> requests = new ArrayList<Buffer>(numBuffers);

        // Request all buffers
        for (int i = 1; i <= numBuffers; i++) {
            requests.add(localBufferPool.requestBuffer());
        }

        assertThat(getNumRequestedFromMemorySegmentPool()).isEqualTo(numBuffers);

        // Reduce the number of buffers in the local pool
        localBufferPool.setNumBuffers(numBuffers / 2);

        // Need to wait until we recycle the buffers
        assertThat(getNumRequestedFromMemorySegmentPool()).isEqualTo(numBuffers);

        for (int i = 1; i < numBuffers / 2; i++) {
            requests.remove(0).recycleBuffer();
            assertThat(getNumRequestedFromMemorySegmentPool()).isEqualTo(numBuffers - i);
        }

        for (Buffer buffer : requests) {
            buffer.recycleBuffer();
        }
    }

    @Test
    public void testRecycleExcessBuffersAfterChangingNumBuffers() {
        localBufferPool.setNumBuffers(numBuffers);

        List<Buffer> requests = new ArrayList<Buffer>(numBuffers);

        // Request all buffers
        for (int i = 1; i <= numBuffers; i++) {
            requests.add(localBufferPool.requestBuffer());
        }

        // Recycle all
        for (Buffer buffer : requests) {
            buffer.recycleBuffer();
        }

        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(numBuffers);

        localBufferPool.setNumBuffers(numBuffers / 2);

        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(numBuffers / 2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetLessThanRequiredNumBuffers() {
        localBufferPool.setNumBuffers(1);

        localBufferPool.setNumBuffers(0);
    }

    // ------------------------------------------------------------------------
    // Pending requests and integration with buffer futures
    // ------------------------------------------------------------------------

    @Test
    public void testPendingRequestWithListenersAfterRecycle() {
        CountBufferListener listener1 = new CountBufferListener();
        CountBufferListener listener2 = new CountBufferListener();

        Buffer available = localBufferPool.requestBuffer();

        assertThat(localBufferPool.requestBuffer()).isNull();

        assertThat(localBufferPool.addBufferListener(listener1)).isTrue();
        assertThat(localBufferPool.addBufferListener(listener2)).isTrue();

        // Recycle the buffer to notify both of the above listeners once
        checkNotNull(available).recycleBuffer();

        assertThat(listener1.getCount()).isEqualTo(1);
        assertThat(listener1.getCount()).isEqualTo(1);

        assertThat(localBufferPool.addBufferListener(listener1)).isFalse();
        assertThat(localBufferPool.addBufferListener(listener2)).isFalse();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCancelPendingRequestsAfterDestroy() {
        BufferListener listener = Mockito.mock(BufferListener.class);

        localBufferPool.setNumBuffers(1);

        Buffer available = localBufferPool.requestBuffer();
        Buffer unavailable = localBufferPool.requestBuffer();

        assertThat(unavailable).isNull();

        localBufferPool.addBufferListener(listener);

        localBufferPool.lazyDestroy();

        available.recycleBuffer();

        verify(listener, times(1)).notifyBufferDestroyed();
    }

    // ------------------------------------------------------------------------
    // Concurrent requests
    // ------------------------------------------------------------------------

    @Test
    @SuppressWarnings("unchecked")
    public void testConcurrentRequestRecycle() throws ExecutionException, InterruptedException {
        int numConcurrentTasks = 128;
        int numBuffersToRequestPerTask = 1024;

        localBufferPool.setNumBuffers(numConcurrentTasks);

        Future<Boolean>[] taskResults = new Future[numConcurrentTasks];
        for (int i = 0; i < numConcurrentTasks; i++) {
            taskResults[i] =
                    executor.submit(
                            new BufferRequesterTask(localBufferPool, numBuffersToRequestPerTask));
        }

        for (int i = 0; i < numConcurrentTasks; i++) {
            assertThat(taskResults[i].get()).isTrue();
        }
    }

    @Test
    public void testBoundedBuffer() throws Exception {
        localBufferPool.lazyDestroy();

        localBufferPool = new LocalBufferPool(networkBufferPool, 1, 2);
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(1);
        assertThat(localBufferPool.getMaxNumberOfMemorySegments()).isEqualTo(2);

        Buffer buffer1, buffer2;

        // check min number of buffers:
        localBufferPool.setNumBuffers(1);
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(1);
        assertThat(buffer1 = localBufferPool.requestBuffer()).isNotNull();
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(0);
        assertThat(localBufferPool.requestBuffer()).isNull();
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(0);
        buffer1.recycleBuffer();
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(1);

        // check max number of buffers:
        localBufferPool.setNumBuffers(2);
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(1);
        assertThat(buffer1 = localBufferPool.requestBuffer()).isNotNull();
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(1);
        assertThat(buffer2 = localBufferPool.requestBuffer()).isNotNull();
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(0);
        assertThat(localBufferPool.requestBuffer()).isNull();
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(0);
        buffer1.recycleBuffer();
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(1);
        buffer2.recycleBuffer();
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(2);

        // try to set too large buffer size:
        localBufferPool.setNumBuffers(3);
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(2);
        assertThat(buffer1 = localBufferPool.requestBuffer()).isNotNull();
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(1);
        assertThat(buffer2 = localBufferPool.requestBuffer()).isNotNull();
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(0);
        assertThat(localBufferPool.requestBuffer()).isNull();
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(0);
        buffer1.recycleBuffer();
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(1);
        buffer2.recycleBuffer();
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(2);

        // decrease size again
        localBufferPool.setNumBuffers(1);
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(1);
        assertThat(buffer1 = localBufferPool.requestBuffer()).isNotNull();
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(0);
        assertThat(localBufferPool.requestBuffer()).isNull();
        buffer1.recycleBuffer();
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(1);
    }

    /** Moves around availability of a {@link LocalBufferPool} with varying capacity. */
    @Test
    public void testMaxBuffersPerChannelAndAvailability() throws Exception {
        localBufferPool.lazyDestroy();
        localBufferPool = new LocalBufferPool(networkBufferPool, 1, Integer.MAX_VALUE, 3, 2);
        localBufferPool.setNumBuffers(10);

        assertThat(localBufferPool.getAvailableFuture().isDone()).isTrue();

        // request one segment from subpartitin-0 and subpartition-1 respectively
        final BufferBuilder bufferBuilder01 = localBufferPool.requestBufferBuilderBlocking(0);
        final BufferBuilder bufferBuilder11 = localBufferPool.requestBufferBuilderBlocking(1);
        assertThat(localBufferPool.getAvailableFuture().isDone()).isTrue();

        // request one segment from subpartition-0
        final BufferBuilder bufferBuilder02 = localBufferPool.requestBufferBuilderBlocking(0);
        assertThat(localBufferPool.getAvailableFuture().isDone()).isFalse();

        assertThat(localBufferPool.requestBufferBuilder(0)).isNull();
        final BufferBuilder bufferBuilder21 = localBufferPool.requestBufferBuilderBlocking(2);
        final BufferBuilder bufferBuilder22 = localBufferPool.requestBufferBuilderBlocking(2);
        assertThat(localBufferPool.getAvailableFuture().isDone()).isFalse();

        // recycle segments
        bufferBuilder11.close();
        assertThat(localBufferPool.getAvailableFuture().isDone()).isFalse();
        bufferBuilder21.close();
        assertThat(localBufferPool.getAvailableFuture().isDone()).isFalse();
        bufferBuilder02.close();
        assertThat(localBufferPool.getAvailableFuture().isDone()).isTrue();
        bufferBuilder01.close();
        assertThat(localBufferPool.getAvailableFuture().isDone()).isTrue();
        bufferBuilder22.close();
        assertThat(localBufferPool.getAvailableFuture().isDone()).isTrue();
    }

    @Test
    public void testIsAvailableOrNot() throws InterruptedException {

        // the local buffer pool should be in available state initially
        assertThat(localBufferPool.isAvailable()).isTrue();

        // request one buffer
        try (BufferBuilder bufferBuilder =
                checkNotNull(localBufferPool.requestBufferBuilderBlocking())) {
            CompletableFuture<?> availableFuture = localBufferPool.getAvailableFuture();
            assertThat(availableFuture.isDone()).isFalse();

            // set the pool size
            final int numLocalBuffers = 5;
            localBufferPool.setNumBuffers(numLocalBuffers);
            assertThat(availableFuture.isDone()).isTrue();
            assertThat(localBufferPool.isAvailable()).isTrue();

            // drain the local buffer pool
            final Deque<Buffer> buffers = new ArrayDeque<>(LocalBufferPoolTest.numBuffers);
            for (int i = 0; i < numLocalBuffers - 1; i++) {
                assertThat(localBufferPool.isAvailable()).isTrue();
                buffers.add(checkNotNull(localBufferPool.requestBuffer()));
            }
            assertThat(localBufferPool.isAvailable()).isFalse();

            buffers.pop().recycleBuffer();
            assertThat(localBufferPool.isAvailable()).isTrue();

            // recycle the requested segments to global buffer pool
            for (final Buffer buffer : buffers) {
                buffer.recycleBuffer();
            }
            assertThat(localBufferPool.isAvailable()).isTrue();

            // scale down (first buffer still taken), but there should still be one segment locally
            // available
            localBufferPool.setNumBuffers(2);
            assertThat(localBufferPool.isAvailable()).isTrue();

            final Buffer buffer2 = checkNotNull(localBufferPool.requestBuffer());
            assertThat(localBufferPool.isAvailable()).isFalse();

            buffer2.recycleBuffer();
            assertThat(localBufferPool.isAvailable()).isTrue();

            // reset the pool size
            localBufferPool.setNumBuffers(1);
            assertThat(localBufferPool.getAvailableFuture().isDone()).isFalse();
            // recycle the requested buffer
        }

        assertThat(localBufferPool.isAvailable()).isTrue();
        assertThat(localBufferPool.getAvailableFuture().isDone()).isTrue();
    }

    /** For FLINK-20547: https://issues.apache.org/jira/browse/FLINK-20547. */
    @Test
    public void testConsistentAvailability() throws Exception {
        NetworkBufferPool globalPool = new TestNetworkBufferPool(numBuffers, memorySegmentSize);
        try {
            BufferPool localPool = new LocalBufferPool(globalPool, 1);
            MemorySegment segment = localPool.requestMemorySegmentBlocking();
            localPool.setNumBuffers(2);

            localPool.recycle(segment);
            localPool.lazyDestroy();
        } finally {
            globalPool.destroy();
        }
    }

    // ------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------

    private int getNumRequestedFromMemorySegmentPool() {
        return networkBufferPool.getTotalNumberOfMemorySegments()
                - networkBufferPool.getNumberOfAvailableMemorySegments();
    }

    private static class CountBufferListener implements BufferListener {

        private final AtomicInteger times = new AtomicInteger(0);

        @Override
        public boolean notifyBufferAvailable(Buffer buffer) {
            times.incrementAndGet();
            buffer.recycleBuffer();
            return true;
        }

        @Override
        public void notifyBufferDestroyed() {}

        int getCount() {
            return times.get();
        }
    }

    private static class BufferRequesterTask implements Callable<Boolean> {

        private final BufferProvider bufferProvider;

        private final int numBuffersToRequest;

        private BufferRequesterTask(BufferProvider bufferProvider, int numBuffersToRequest) {
            this.bufferProvider = bufferProvider;
            this.numBuffersToRequest = numBuffersToRequest;
        }

        @Override
        public Boolean call() throws Exception {
            try {
                for (int i = 0; i < numBuffersToRequest; i++) {
                    Buffer buffer = checkNotNull(bufferProvider.requestBuffer());
                    buffer.recycleBuffer();
                }
            } catch (Throwable t) {
                return false;
            }

            return true;
        }
    }

    private static class TestNetworkBufferPool extends NetworkBufferPool {

        private int requestCounter;

        public TestNetworkBufferPool(int numberOfSegmentsToAllocate, int segmentSize) {
            super(numberOfSegmentsToAllocate, segmentSize);
        }

        @Nullable
        @Override
        public MemorySegment requestMemorySegment() {
            if (requestCounter++ == 1) {
                return null;
            }
            return super.requestMemorySegment();
        }
    }
}
