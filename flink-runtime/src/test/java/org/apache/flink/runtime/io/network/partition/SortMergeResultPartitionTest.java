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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.disk.FileChannelManagerImpl;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SortMergeResultPartition}. */
public class SortMergeResultPartitionTest extends TestLogger {

    private static final int bufferSize = 1024;

    private static final int totalBuffers = 1000;

    private static final int totalBytes = 32 * 1024 * 1024;

    private static final int numThreads = 4;

    private final TestBufferAvailabilityListener listener = new TestBufferAvailabilityListener();

    private FileChannelManager fileChannelManager;

    private NetworkBufferPool globalPool;

    private BatchShuffleReadBufferPool readBufferPool;

    private ExecutorService readIOExecutor;

    @Rule public final TemporaryFolder tmpFolder = new TemporaryFolder();

    @Rule public Timeout timeout = new Timeout(60, TimeUnit.SECONDS);

    @Before
    public void setUp() {
        fileChannelManager =
                new FileChannelManagerImpl(new String[] {tmpFolder.getRoot().getPath()}, "testing");
        globalPool = new NetworkBufferPool(totalBuffers, bufferSize);
        readBufferPool = new BatchShuffleReadBufferPool(totalBytes, bufferSize);
        readIOExecutor = Executors.newFixedThreadPool(numThreads);
    }

    @After
    public void shutdown() throws Exception {
        fileChannelManager.close();
        globalPool.destroy();
        readBufferPool.destroy();
        readIOExecutor.shutdown();
    }

    @Test
    public void testWriteAndRead() throws Exception {
        int numSubpartitions = 10;
        int numBuffers = 100;
        int numRecords = 1000;
        Random random = new Random();

        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        SortMergeResultPartition partition =
                createSortMergedPartition(numSubpartitions, bufferPool);

        Queue<PartitionSortedBufferTest.DataAndType>[] dataWritten = new Queue[numSubpartitions];
        Queue<Buffer>[] buffersRead = new Queue[numSubpartitions];
        for (int i = 0; i < numSubpartitions; ++i) {
            dataWritten[i] = new ArrayDeque<>();
            buffersRead[i] = new ArrayDeque<>();
        }

        int[] numBytesWritten = new int[numSubpartitions];
        int[] numBytesRead = new int[numSubpartitions];
        Arrays.fill(numBytesWritten, 0);
        Arrays.fill(numBytesRead, 0);

        for (int i = 0; i < numRecords; ++i) {
            ByteBuffer record = generateRandomData(random.nextInt(2 * bufferSize) + 1, random);
            boolean isBroadCast = random.nextBoolean();

            if (isBroadCast) {
                partition.broadcastRecord(record);
                for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
                    recordDataWritten(
                            record,
                            dataWritten,
                            subpartition,
                            numBytesWritten,
                            DataType.DATA_BUFFER);
                }
            } else {
                int subpartition = random.nextInt(numSubpartitions);
                partition.emitRecord(record, subpartition);
                recordDataWritten(
                        record, dataWritten, subpartition, numBytesWritten, DataType.DATA_BUFFER);
            }
        }

        partition.finish();
        partition.close();
        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            ByteBuffer record = EventSerializer.toSerializedEvent(EndOfPartitionEvent.INSTANCE);
            recordDataWritten(
                    record, dataWritten, subpartition, numBytesWritten, DataType.EVENT_BUFFER);
        }

        ResultSubpartitionView[] views = createSubpartitionViews(partition, numSubpartitions);
        readData(
                views,
                bufferWithChannel -> {
                    Buffer buffer = bufferWithChannel.getBuffer();
                    int subpartition = bufferWithChannel.getChannelIndex();

                    int numBytes = buffer.readableBytes();
                    numBytesRead[subpartition] += numBytes;

                    MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(numBytes);
                    segment.put(0, buffer.getNioBufferReadable(), numBytes);
                    buffersRead[subpartition].add(
                            new NetworkBuffer(
                                    segment, (buf) -> {}, buffer.getDataType(), numBytes));
                });
        PartitionSortedBufferTest.checkWriteReadResult(
                numSubpartitions, numBytesWritten, numBytesRead, dataWritten, buffersRead);
    }

    private void recordDataWritten(
            ByteBuffer record,
            Queue<PartitionSortedBufferTest.DataAndType>[] dataWritten,
            int subpartition,
            int[] numBytesWritten,
            Buffer.DataType dataType) {
        record.rewind();
        dataWritten[subpartition].add(new PartitionSortedBufferTest.DataAndType(record, dataType));
        numBytesWritten[subpartition] += record.remaining();
    }

    private ByteBuffer generateRandomData(int dataSize, Random random) {
        byte[] dataWritten = new byte[dataSize];
        random.nextBytes(dataWritten);
        return ByteBuffer.wrap(dataWritten);
    }

    private long readData(
            ResultSubpartitionView[] views, Consumer<BufferWithChannel> bufferProcessor)
            throws Exception {
        int dataSize = 0;
        int numEndOfPartitionEvents = 0;

        while (numEndOfPartitionEvents < views.length) {
            listener.waitForData();
            for (int subpartition = 0; subpartition < views.length; ++subpartition) {
                ResultSubpartitionView view = views[subpartition];
                ResultSubpartition.BufferAndBacklog bufferAndBacklog = view.getNextBuffer();
                while (bufferAndBacklog != null) {
                    Buffer buffer = bufferAndBacklog.buffer();
                    bufferProcessor.accept(new BufferWithChannel(buffer, subpartition));
                    dataSize += buffer.readableBytes();
                    buffer.recycleBuffer();

                    if (!buffer.isBuffer()) {
                        ++numEndOfPartitionEvents;
                        assertThat(view.getAvailabilityAndBacklog(Integer.MAX_VALUE).isAvailable())
                                .isFalse();
                        view.releaseAllResources();
                    }
                    bufferAndBacklog = view.getNextBuffer();
                }
            }
        }
        return dataSize;
    }

    private ResultSubpartitionView[] createSubpartitionViews(
            SortMergeResultPartition partition, int numSubpartitions) throws Exception {
        ResultSubpartitionView[] views = new ResultSubpartitionView[numSubpartitions];
        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            views[subpartition] = partition.createSubpartitionView(subpartition, listener);
        }
        return views;
    }

    @Test
    public void testWriteLargeRecord() throws Exception {
        int numBuffers = 100;
        int numWriteBuffers = numBuffers / 2;
        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        SortMergeResultPartition partition = createSortMergedPartition(10, bufferPool);

        ByteBuffer recordWritten = generateRandomData(bufferSize * numBuffers, new Random());
        partition.emitRecord(recordWritten, 0);
        assertThat(bufferPool.bestEffortGetNumOfUsedBuffers()).isEqualTo(numWriteBuffers);

        partition.finish();
        partition.close();

        ResultSubpartitionView view = partition.createSubpartitionView(0, listener);
        ByteBuffer recordRead = ByteBuffer.allocate(bufferSize * numBuffers);
        readData(
                new ResultSubpartitionView[] {view},
                bufferWithChannel -> {
                    Buffer buffer = bufferWithChannel.getBuffer();
                    if (buffer.isBuffer()) {
                        recordRead.put(buffer.getNioBufferReadable());
                    }
                });
        recordWritten.rewind();
        recordRead.flip();
        assertThat(recordRead).isEqualTo(recordWritten);
    }

    @Test
    public void testDataBroadcast() throws Exception {
        int numSubpartitions = 10;
        int numBuffers = 100;
        int numRecords = 10000;

        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        SortMergeResultPartition partition =
                createSortMergedPartition(numSubpartitions, bufferPool);

        for (int i = 0; i < numRecords; ++i) {
            ByteBuffer record = generateRandomData(bufferSize, new Random());
            partition.broadcastRecord(record);
        }
        partition.finish();
        partition.close();

        int eventSize = EventSerializer.toSerializedEvent(EndOfPartitionEvent.INSTANCE).remaining();
        long dataSize = numSubpartitions * numRecords * bufferSize + numSubpartitions * eventSize;
        assertThat(partition.getResultFile()).isNotNull();
        assertThat(checkNotNull(fileChannelManager.getPaths()[0].list()).length).isEqualTo(2);
        for (File file : checkNotNull(fileChannelManager.getPaths()[0].listFiles())) {
            if (file.getName().endsWith(PartitionedFile.DATA_FILE_SUFFIX)) {
                assertThat(file.length() < numSubpartitions * numRecords * bufferSize).isTrue();
            }
        }

        ResultSubpartitionView[] views = createSubpartitionViews(partition, numSubpartitions);
        long dataRead = readData(views, (ignored) -> {});
        assertThat(dataRead).isEqualTo(dataSize);
    }

    @Test
    public void testFlush() throws Exception {
        int numBuffers = 10;
        int numWriteBuffers = numBuffers / 2;
        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        SortMergeResultPartition partition = createSortMergedPartition(10, bufferPool);
        assertThat(bufferPool.bestEffortGetNumOfUsedBuffers()).isEqualTo(numWriteBuffers);

        partition.emitRecord(ByteBuffer.allocate(bufferSize), 0);
        partition.emitRecord(ByteBuffer.allocate(bufferSize), 1);
        assertThat(bufferPool.bestEffortGetNumOfUsedBuffers()).isEqualTo(3 + numWriteBuffers);

        partition.flush(0);
        assertThat(bufferPool.bestEffortGetNumOfUsedBuffers()).isEqualTo(numWriteBuffers);

        partition.emitRecord(ByteBuffer.allocate(bufferSize), 2);
        partition.emitRecord(ByteBuffer.allocate(bufferSize), 3);
        assertThat(bufferPool.bestEffortGetNumOfUsedBuffers()).isEqualTo(3 + numWriteBuffers);

        partition.flushAll();
        assertThat(bufferPool.bestEffortGetNumOfUsedBuffers()).isEqualTo(numWriteBuffers);

        assertThat(partition.getResultFile()).isNull();
        partition.finish();
        assertThat(partition.getResultFile().getNumRegions()).isEqualTo(3);

        partition.close();
    }

    @Test(expected = IllegalStateException.class)
    public void testReleaseWhileWriting() throws Exception {
        int numBuffers = 10;
        int numWriteBuffers = numBuffers / 2;
        int numBuffersForSort = numBuffers - numWriteBuffers;

        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        SortMergeResultPartition partition = createSortMergedPartition(10, bufferPool);
        assertThat(bufferPool.bestEffortGetNumOfUsedBuffers()).isEqualTo(numWriteBuffers);

        partition.emitRecord(ByteBuffer.allocate(bufferSize * (numBuffersForSort - 1)), 0);
        partition.emitRecord(ByteBuffer.allocate(bufferSize * (numBuffersForSort - 1)), 1);

        partition.emitRecord(ByteBuffer.allocate(bufferSize), 2);
        assertThat(partition.getResultFile()).isNull();
        assertThat(fileChannelManager.getPaths()[0].list().length).isEqualTo(2);

        partition.release();
        try {
            partition.emitRecord(ByteBuffer.allocate(bufferSize * numBuffers), 2);
        } catch (IllegalStateException exception) {
            assertThat(fileChannelManager.getPaths()[0].list().length).isEqualTo(0);

            throw exception;
        }
    }

    @Test
    public void testRelease() throws Exception {
        int numBuffers = 10;
        int numWriteBuffers = numBuffers / 2;
        int numBuffersForSort = numBuffers - numWriteBuffers;

        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        SortMergeResultPartition partition = createSortMergedPartition(10, bufferPool);
        assertThat(bufferPool.bestEffortGetNumOfUsedBuffers()).isEqualTo(numWriteBuffers);

        partition.emitRecord(ByteBuffer.allocate(bufferSize * (numBuffersForSort - 1)), 0);
        partition.emitRecord(ByteBuffer.allocate(bufferSize * (numBuffersForSort - 1)), 1);
        partition.finish();
        partition.close();

        assertThat(partition.getResultFile().getNumRegions()).isEqualTo(3);
        assertThat(checkNotNull(fileChannelManager.getPaths()[0].list()).length).isEqualTo(2);

        ResultSubpartitionView view = partition.createSubpartitionView(0, listener);
        partition.release();

        while (!view.isReleased()) {
            ResultSubpartition.BufferAndBacklog bufferAndBacklog = view.getNextBuffer();
            if (bufferAndBacklog != null) {
                bufferAndBacklog.buffer().recycleBuffer();
            }
        }

        // wait util partition file is released
        while (partition.getResultFile() != null) {
            Thread.sleep(100);
        }
        assertThat(checkNotNull(fileChannelManager.getPaths()[0].list()).length).isEqualTo(0);
    }

    @Test
    public void testCloseReleasesAllBuffers() throws Exception {
        int numBuffers = 100;
        int numWriteBuffers = numBuffers / 2;
        int numBuffersForSort = numBuffers - numWriteBuffers;

        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        SortMergeResultPartition partition = createSortMergedPartition(10, bufferPool);
        assertThat(bufferPool.bestEffortGetNumOfUsedBuffers()).isEqualTo(numWriteBuffers);

        partition.emitRecord(ByteBuffer.allocate(bufferSize * (numBuffersForSort - 1)), 5);
        assertThat(bufferPool.bestEffortGetNumOfUsedBuffers()).isEqualTo(numBuffers);

        partition.close();
        assertThat(bufferPool.isDestroyed()).isTrue();
        assertThat(globalPool.getNumberOfAvailableMemorySegments()).isEqualTo(totalBuffers);
    }

    @Test(expected = IllegalStateException.class)
    public void testReadUnfinishedPartition() throws Exception {
        BufferPool bufferPool = globalPool.createBufferPool(10, 10);
        try {
            SortMergeResultPartition partition = createSortMergedPartition(10, bufferPool);
            partition.createSubpartitionView(0, listener);
        } finally {
            bufferPool.lazyDestroy();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testReadReleasedPartition() throws Exception {
        BufferPool bufferPool = globalPool.createBufferPool(10, 10);
        try {
            SortMergeResultPartition partition = createSortMergedPartition(10, bufferPool);
            partition.finish();
            partition.release();
            partition.createSubpartitionView(0, listener);
        } finally {
            bufferPool.lazyDestroy();
        }
    }

    @Test
    public void testNumBytesProducedCounterForUnicast() throws IOException {
        testNumBytesProducedCounter(false);
    }

    @Test
    public void testNumBytesProducedCounterForBroadcast() throws IOException {
        testNumBytesProducedCounter(true);
    }

    private void testNumBytesProducedCounter(boolean isBroadcast) throws IOException {

        int numSubpartitions = 10;

        BufferPool bufferPool = globalPool.createBufferPool(10, 10);
        SortMergeResultPartition partition =
                createSortMergedPartition(numSubpartitions, bufferPool);

        if (isBroadcast) {
            partition.broadcastRecord(ByteBuffer.allocate(bufferSize));
            partition.flushAll();

            assertThat(partition.numBytesProduced.getCount()).isEqualTo(bufferSize);
            assertThat(partition.numBytesOut.getCount()).isEqualTo(numSubpartitions * bufferSize);
        } else {
            partition.emitRecord(ByteBuffer.allocate(bufferSize), 0);
            partition.flushAll();

            assertThat(partition.numBytesProduced.getCount()).isEqualTo(bufferSize);
            assertThat(partition.numBytesOut.getCount()).isEqualTo(bufferSize);
        }
    }

    private SortMergeResultPartition createSortMergedPartition(
            int numSubpartitions, BufferPool bufferPool) throws IOException {
        return createSortMergedPartition(numSubpartitions, bufferPool, readBufferPool);
    }

    private SortMergeResultPartition createSortMergedPartition(
            int numSubpartitions, BufferPool bufferPool, BatchShuffleReadBufferPool readBufferPool)
            throws IOException {
        SortMergeResultPartition sortMergedResultPartition =
                new SortMergeResultPartition(
                        "SortMergedResultPartitionTest",
                        0,
                        new ResultPartitionID(),
                        ResultPartitionType.BLOCKING,
                        numSubpartitions,
                        numSubpartitions,
                        readBufferPool,
                        readIOExecutor,
                        new ResultPartitionManager(),
                        fileChannelManager.createChannel().getPath(),
                        null,
                        () -> bufferPool);
        sortMergedResultPartition.setup();
        return sortMergedResultPartition;
    }

    private static final class TestBufferAvailabilityListener
            implements BufferAvailabilityListener {

        private int numNotifications;

        @Override
        public synchronized void notifyDataAvailable() {
            if (numNotifications == 0) {
                notifyAll();
            }
            ++numNotifications;
        }

        public synchronized void waitForData() throws InterruptedException {
            if (numNotifications == 0) {
                wait();
            }
            numNotifications = 0;
        }
    }
}
