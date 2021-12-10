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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SortMergeResultPartitionReadScheduler}. */
public class SortMergeResultPartitionReadSchedulerTest extends TestLogger {

    private static final int bufferSize = 1024;

    private static final byte[] dataBytes = new byte[bufferSize];

    private static final int totalBytes = bufferSize;

    private static final int numThreads = 4;

    private static final int numSubpartitions = 10;

    private static final int numBuffersPerSubpartition = 10;

    private PartitionedFile partitionedFile;

    private BatchShuffleReadBufferPool bufferPool;

    private ExecutorService executor;

    private SortMergeResultPartitionReadScheduler readScheduler;

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule public Timeout timeout = new Timeout(60, TimeUnit.SECONDS);

    @Before
    public void before() throws Exception {
        Random random = new Random();
        random.nextBytes(dataBytes);
        partitionedFile =
                PartitionTestUtils.createPartitionedFile(
                        temporaryFolder.newFile().getAbsolutePath(),
                        numSubpartitions,
                        numBuffersPerSubpartition,
                        bufferSize,
                        dataBytes);
        bufferPool = new BatchShuffleReadBufferPool(totalBytes, bufferSize);
        executor = Executors.newFixedThreadPool(numThreads);
        readScheduler = new SortMergeResultPartitionReadScheduler(bufferPool, executor, this);
    }

    @After
    public void after() {
        partitionedFile.deleteQuietly();
        bufferPool.destroy();
        executor.shutdown();
    }

    @Test
    public void testCreateSubpartitionReader() throws Exception {
        SortMergeSubpartitionReader subpartitionReader =
                readScheduler.crateSubpartitionReader(
                        new NoOpBufferAvailablityListener(), 0, partitionedFile);

        assertThat(readScheduler.isRunning()).isTrue();
        assertThat(readScheduler.getDataFileChannel().isOpen()).isTrue();
        assertThat(readScheduler.getIndexFileChannel().isOpen()).isTrue();

        int numBuffersRead = 0;
        while (numBuffersRead < numBuffersPerSubpartition) {
            ResultSubpartition.BufferAndBacklog bufferAndBacklog =
                    subpartitionReader.getNextBuffer();
            if (bufferAndBacklog != null) {
                Buffer buffer = bufferAndBacklog.buffer();
                assertThat(buffer.getNioBufferReadable()).isEqualTo(ByteBuffer.wrap(dataBytes));
                buffer.recycleBuffer();
                ++numBuffersRead;
            }
        }
    }

    @Test
    public void testOnSubpartitionReaderError() throws Exception {
        SortMergeSubpartitionReader subpartitionReader =
                readScheduler.crateSubpartitionReader(
                        new NoOpBufferAvailablityListener(), 0, partitionedFile);

        subpartitionReader.releaseAllResources();
        waitUntilReadFinish();
        assertAllResourcesReleased();
    }

    @Test
    public void testReleaseWhileReading() throws Exception {
        SortMergeSubpartitionReader subpartitionReader =
                readScheduler.crateSubpartitionReader(
                        new NoOpBufferAvailablityListener(), 0, partitionedFile);

        Thread.sleep(1000);
        readScheduler.release();

        assertThat(subpartitionReader.getFailureCause()).isNotNull();
        assertThat(subpartitionReader.isReleased()).isTrue();
        assertThat(subpartitionReader.unsynchronizedGetNumberOfQueuedBuffers()).isEqualTo(0);
        assertThat(subpartitionReader.getAvailabilityAndBacklog(0).isAvailable()).isTrue();

        readScheduler.getReleaseFuture().get();
        assertAllResourcesReleased();
    }

    @Test(expected = IllegalStateException.class)
    public void testCreateSubpartitionReaderAfterReleased() throws Exception {
        readScheduler.release();
        try {
            readScheduler.crateSubpartitionReader(
                    new NoOpBufferAvailablityListener(), 0, partitionedFile);
        } finally {
            assertAllResourcesReleased();
        }
    }

    @Test
    public void testOnDataReadError() throws Exception {
        SortMergeSubpartitionReader subpartitionReader =
                readScheduler.crateSubpartitionReader(
                        new NoOpBufferAvailablityListener(), 0, partitionedFile);

        // close file channel to trigger data read exception
        readScheduler.getDataFileChannel().close();

        while (!subpartitionReader.isReleased()) {
            ResultSubpartition.BufferAndBacklog bufferAndBacklog =
                    subpartitionReader.getNextBuffer();
            if (bufferAndBacklog != null) {
                bufferAndBacklog.buffer().recycleBuffer();
            }
        }

        waitUntilReadFinish();
        assertThat(subpartitionReader.getFailureCause()).isNotNull();
        assertThat(subpartitionReader.getAvailabilityAndBacklog(0).isAvailable()).isTrue();
        assertAllResourcesReleased();
    }

    @Test
    public void testOnReadBufferRequestError() throws Exception {
        SortMergeSubpartitionReader subpartitionReader =
                readScheduler.crateSubpartitionReader(
                        new NoOpBufferAvailablityListener(), 0, partitionedFile);

        bufferPool.destroy();
        waitUntilReadFinish();

        assertThat(subpartitionReader.isReleased()).isTrue();
        assertThat(subpartitionReader.getFailureCause()).isNotNull();
        assertThat(subpartitionReader.getAvailabilityAndBacklog(0).isAvailable()).isTrue();
        assertAllResourcesReleased();
    }

    private void assertAllResourcesReleased() {
        assertThat(readScheduler.getDataFileChannel()).isNull();
        assertThat(readScheduler.getIndexFileChannel()).isNull();
        assertThat(readScheduler.isRunning()).isFalse();
        assertThat(readScheduler.getNumPendingReaders()).isEqualTo(0);

        if (!bufferPool.isDestroyed()) {
            assertThat(bufferPool.getAvailableBuffers()).isEqualTo(bufferPool.getNumTotalBuffers());
        }
    }

    private void waitUntilReadFinish() throws Exception {
        while (readScheduler.isRunning()) {
            Thread.sleep(100);
        }
    }
}
