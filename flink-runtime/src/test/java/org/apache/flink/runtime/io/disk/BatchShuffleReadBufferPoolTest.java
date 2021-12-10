/*
 * Copyright 2012 The Netty Project
 * Copy from netty 4.1.32.Final
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.io.disk;

import org.apache.flink.core.memory.MemorySegment;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BatchShuffleReadBufferPool}. */
public class BatchShuffleReadBufferPoolTest {

    @Rule public Timeout timeout = new Timeout(60, TimeUnit.SECONDS);

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalTotalBytes() {
        createBufferPool(0, 1024);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalBufferSize() {
        createBufferPool(32 * 1024 * 1024, 0);
    }

    @Test
    public void testLargeTotalBytes() {
        BatchShuffleReadBufferPool bufferPool = createBufferPool(Long.MAX_VALUE, 1024);
        assertThat(bufferPool.getNumTotalBuffers()).isEqualTo(Integer.MAX_VALUE);
        bufferPool.destroy();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTotalBytesSmallerThanBufferSize() {
        createBufferPool(4096, 32 * 1024);
    }

    @Test
    public void testBufferCalculation() {
        long totalBytes = 32 * 1024 * 1024;
        for (int bufferSize = 4 * 1024; bufferSize <= totalBytes; bufferSize += 1024) {
            BatchShuffleReadBufferPool bufferPool = createBufferPool(totalBytes, bufferSize);

            assertThat(bufferPool.getTotalBytes()).isEqualTo(totalBytes);
            assertThat(bufferPool.getNumTotalBuffers()).isEqualTo(totalBytes / bufferSize);
            assertThat(bufferPool.getNumBuffersPerRequest() <= bufferPool.getNumTotalBuffers())
                    .isTrue();
            assertThat(bufferPool.getNumBuffersPerRequest() > 0).isTrue();
        }
    }

    @Test
    public void testRequestBuffers() throws Exception {
        BatchShuffleReadBufferPool bufferPool = createBufferPool();
        List<MemorySegment> buffers = new ArrayList<>();

        try {
            buffers.addAll(bufferPool.requestBuffers());
            assertThat(buffers.size()).isEqualTo(bufferPool.getNumBuffersPerRequest());
        } finally {
            bufferPool.recycle(buffers);
            bufferPool.destroy();
        }
    }

    @Test
    public void testRecycle() throws Exception {
        BatchShuffleReadBufferPool bufferPool = createBufferPool();
        List<MemorySegment> buffers = bufferPool.requestBuffers();

        bufferPool.recycle(buffers);
        assertThat(bufferPool.getAvailableBuffers()).isEqualTo(bufferPool.getNumTotalBuffers());
    }

    @Test
    public void testBufferFulfilledByRecycledBuffers() throws Exception {
        int numRequestThreads = 2;
        AtomicReference<Throwable> exception = new AtomicReference<>();
        BatchShuffleReadBufferPool bufferPool = createBufferPool();
        Map<Object, List<MemorySegment>> buffers = new ConcurrentHashMap<>();

        try {
            Object[] owners = new Object[] {new Object(), new Object(), new Object(), new Object()};
            for (int i = 0; i < 4; ++i) {
                buffers.put(owners[i], bufferPool.requestBuffers());
            }
            assertThat(bufferPool.getAvailableBuffers()).isEqualTo(0);

            Thread[] requestThreads = new Thread[numRequestThreads];
            for (int i = 0; i < numRequestThreads; ++i) {
                requestThreads[i] =
                        new Thread(
                                () -> {
                                    try {
                                        Object owner = new Object();
                                        List<MemorySegment> allocated = null;
                                        while (allocated == null || allocated.isEmpty()) {
                                            allocated = bufferPool.requestBuffers();
                                        }
                                        buffers.put(owner, allocated);
                                    } catch (Throwable throwable) {
                                        exception.set(throwable);
                                    }
                                });
                requestThreads[i].start();
            }

            // recycle one by one
            for (MemorySegment segment : buffers.remove(owners[0])) {
                bufferPool.recycle(segment);
            }

            // bulk recycle
            bufferPool.recycle(buffers.remove(owners[1]));

            for (Thread requestThread : requestThreads) {
                requestThread.join();
            }

            assertThat(exception.get()).isNull();
            assertThat(bufferPool.getAvailableBuffers()).isEqualTo(0);
            assertThat(buffers.size()).isEqualTo(4);
        } finally {
            for (Object owner : buffers.keySet()) {
                bufferPool.recycle(buffers.remove(owner));
            }
            assertThat(bufferPool.getAvailableBuffers()).isEqualTo(bufferPool.getNumTotalBuffers());
            bufferPool.destroy();
        }
    }

    @Test
    public void testMultipleThreadRequestAndRecycle() throws Exception {
        int numRequestThreads = 10;
        AtomicReference<Throwable> exception = new AtomicReference<>();
        BatchShuffleReadBufferPool bufferPool = createBufferPool();

        try {
            Thread[] requestThreads = new Thread[numRequestThreads];
            for (int i = 0; i < numRequestThreads; ++i) {
                requestThreads[i] =
                        new Thread(
                                () -> {
                                    try {
                                        for (int j = 0; j < 100; ++j) {
                                            List<MemorySegment> buffers =
                                                    bufferPool.requestBuffers();
                                            Thread.sleep(10);
                                            if (j % 2 == 0) {
                                                bufferPool.recycle(buffers);
                                            } else {
                                                for (MemorySegment segment : buffers) {
                                                    bufferPool.recycle(segment);
                                                }
                                            }
                                        }
                                    } catch (Throwable throwable) {
                                        exception.set(throwable);
                                    }
                                });
                requestThreads[i].start();
            }

            for (Thread requestThread : requestThreads) {
                requestThread.join();
            }

            assertThat(exception.get()).isNull();
            assertThat(bufferPool.getAvailableBuffers()).isEqualTo(bufferPool.getNumTotalBuffers());
        } finally {
            bufferPool.destroy();
        }
    }

    @Test
    public void testDestroy() throws Exception {
        BatchShuffleReadBufferPool bufferPool = createBufferPool();
        List<MemorySegment> buffers = bufferPool.requestBuffers();
        bufferPool.recycle(buffers);

        assertThat(bufferPool.isDestroyed()).isFalse();
        assertThat(bufferPool.getAvailableBuffers()).isEqualTo(bufferPool.getNumTotalBuffers());

        buffers = bufferPool.requestBuffers();
        assertThat(bufferPool.getAvailableBuffers())
                .isEqualTo(bufferPool.getNumTotalBuffers() - buffers.size());

        bufferPool.destroy();
        assertThat(bufferPool.isDestroyed()).isTrue();
        assertThat(bufferPool.getAvailableBuffers()).isEqualTo(0);
    }

    @Test(expected = IllegalStateException.class)
    public void testRequestBuffersAfterDestroyed() throws Exception {
        BatchShuffleReadBufferPool bufferPool = createBufferPool();
        bufferPool.requestBuffers();

        bufferPool.destroy();
        bufferPool.requestBuffers();
    }

    @Test
    public void testRecycleAfterDestroyed() throws Exception {
        BatchShuffleReadBufferPool bufferPool = createBufferPool();
        List<MemorySegment> buffers = bufferPool.requestBuffers();
        bufferPool.destroy();

        bufferPool.recycle(buffers);
        assertThat(bufferPool.getAvailableBuffers()).isEqualTo(0);
    }

    @Test
    public void testDestroyWhileBlockingRequest() throws Exception {
        AtomicReference<Throwable> exception = new AtomicReference<>();
        BatchShuffleReadBufferPool bufferPool = createBufferPool();

        Thread requestThread =
                new Thread(
                        () -> {
                            try {
                                while (true) {
                                    bufferPool.requestBuffers();
                                }
                            } catch (Throwable throwable) {
                                exception.set(throwable);
                            }
                        });
        requestThread.start();

        Thread.sleep(1000);
        bufferPool.destroy();
        requestThread.join();

        assertThat(exception.get()).isInstanceOf(IllegalStateException.class);
    }

    private BatchShuffleReadBufferPool createBufferPool(long totalBytes, int bufferSize) {
        return new BatchShuffleReadBufferPool(totalBytes, bufferSize);
    }

    private BatchShuffleReadBufferPool createBufferPool() {
        return createBufferPool(32 * 1024 * 1024, 32 * 1024);
    }
}
