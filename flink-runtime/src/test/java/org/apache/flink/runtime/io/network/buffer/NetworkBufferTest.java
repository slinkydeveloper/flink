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
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.netty.NettyBufferPool;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link NetworkBuffer} class. */
public class NetworkBufferTest extends AbstractByteBufTest {

    /** Upper limit for the max size that is sufficient for all the tests. */
    private static final int MAX_CAPACITY_UPPER_BOUND = 64 * 1024 * 1024;

    private static final NettyBufferPool NETTY_BUFFER_POOL = new NettyBufferPool(1);

    @Override
    protected NetworkBuffer newBuffer(int length, int maxCapacity) {
        return newBuffer(length, maxCapacity, false);
    }

    /**
     * Creates a new buffer for testing.
     *
     * @param length buffer capacity
     * @param maxCapacity buffer maximum capacity (will be used for the underlying {@link
     *     MemorySegment})
     * @param isBuffer whether the buffer should represent data (<tt>true</tt>) or an event
     *     (<tt>false</tt>)
     * @return the buffer
     */
    private static NetworkBuffer newBuffer(int length, int maxCapacity, boolean isBuffer) {
        return newBuffer(length, maxCapacity, isBuffer, FreeingBufferRecycler.INSTANCE);
    }

    /**
     * Creates a new buffer for testing.
     *
     * @param length buffer capacity
     * @param maxCapacity buffer maximum capacity (will be used for the underlying {@link
     *     MemorySegment})
     * @param isBuffer whether the buffer should represent data (<tt>true</tt>) or an event
     *     (<tt>false</tt>)
     * @param recycler the buffer recycler to use
     * @return the buffer
     */
    private static NetworkBuffer newBuffer(
            int length, int maxCapacity, boolean isBuffer, BufferRecycler recycler) {
        final MemorySegment segment =
                MemorySegmentFactory.allocateUnpooledSegment(
                        Math.min(maxCapacity, MAX_CAPACITY_UPPER_BOUND));

        Buffer.DataType dataType =
                isBuffer ? Buffer.DataType.DATA_BUFFER : Buffer.DataType.EVENT_BUFFER;
        NetworkBuffer buffer = new NetworkBuffer(segment, recycler, dataType);
        buffer.capacity(length);
        buffer.setAllocator(NETTY_BUFFER_POOL);

        assertThat(buffer.order()).isSameAs(ByteOrder.BIG_ENDIAN);
        assertThat(buffer.readerIndex()).isEqualTo(0);
        assertThat(buffer.writerIndex()).isEqualTo(0);
        return buffer;
    }

    @Test
    public void testDataBufferIsBuffer() {
        assertThat(newBuffer(1024, 1024, false).isBuffer()).isFalse();
    }

    @Test
    public void testEventBufferIsBuffer() {
        assertThat(newBuffer(1024, 1024, false).isBuffer()).isFalse();
    }

    @Test
    public void testDataBufferTagAsEvent() {
        testTagAsEvent(true);
    }

    @Test
    public void testEventBufferTagAsEvent() {
        testTagAsEvent(false);
    }

    private static void testTagAsEvent(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(1024, 1024, isBuffer);
        buffer.setDataType(Buffer.DataType.EVENT_BUFFER);
        assertThat(buffer.isBuffer()).isFalse();
    }

    @Test
    public void testDataBufferGetMemorySegment() {
        testGetMemorySegment(true);
    }

    @Test
    public void testEventBufferGetMemorySegment() {
        testGetMemorySegment(false);
    }

    private static void testGetMemorySegment(boolean isBuffer) {
        final MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(1024);
        Buffer.DataType dataType =
                isBuffer ? Buffer.DataType.DATA_BUFFER : Buffer.DataType.EVENT_BUFFER;
        NetworkBuffer buffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE, dataType);
        assertThat(buffer.getMemorySegment()).isSameAs(segment);
    }

    @Test
    public void testDataBufferGetRecycler() {
        testGetRecycler(true);
    }

    @Test
    public void testEventBufferGetRecycler() {
        testGetRecycler(false);
    }

    private static void testGetRecycler(boolean isBuffer) {
        BufferRecycler recycler = MemorySegment::free;

        NetworkBuffer dataBuffer = newBuffer(1024, 1024, isBuffer, recycler);
        assertThat(dataBuffer.getRecycler()).isSameAs(recycler);
    }

    @Test
    public void testDataBufferRecycleBuffer() {
        testRecycleBuffer(true);
    }

    @Test
    public void testEventBufferRecycleBuffer() {
        testRecycleBuffer(false);
    }

    /**
     * Tests that {@link NetworkBuffer#recycleBuffer()} and {@link NetworkBuffer#isRecycled()} are
     * coupled and are also consistent with {@link NetworkBuffer#refCnt()}.
     */
    private static void testRecycleBuffer(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(1024, 1024, isBuffer);
        assertThat(buffer.isRecycled()).isFalse();
        buffer.recycleBuffer();
        assertThat(buffer.isRecycled()).isTrue();
        assertThat(buffer.refCnt()).isEqualTo(0);
    }

    @Test
    public void testDataBufferRetainBuffer() {
        testRetainBuffer(true);
    }

    @Test
    public void testEventBufferRetainBuffer() {
        testRetainBuffer(false);
    }

    /**
     * Tests that {@link NetworkBuffer#retainBuffer()} and {@link NetworkBuffer#isRecycled()} are
     * coupled and are also consistent with {@link NetworkBuffer#refCnt()}.
     */
    private static void testRetainBuffer(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(1024, 1024, isBuffer);
        assertThat(buffer.isRecycled()).isFalse();
        buffer.retainBuffer();
        assertThat(buffer.isRecycled()).isFalse();
        assertThat(buffer.refCnt()).isEqualTo(2);
    }

    @Test
    public void testDataBufferCreateSlice1() {
        testCreateSlice1(true);
    }

    @Test
    public void testEventBufferCreateSlice1() {
        testCreateSlice1(false);
    }

    private static void testCreateSlice1(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(1024, 1024, isBuffer);
        buffer.setSize(10); // fake some data
        ReadOnlySlicedNetworkBuffer slice = buffer.readOnlySlice();

        assertThat(slice.getReaderIndex()).isEqualTo(0);
        assertThat(slice.getSize()).isEqualTo(10);
        assertThat(slice.unwrap().unwrap()).isSameAs(buffer);

        // slice indices should be independent:
        buffer.setSize(8);
        buffer.setReaderIndex(2);
        assertThat(slice.getReaderIndex()).isEqualTo(0);
        assertThat(slice.getSize()).isEqualTo(10);
    }

    @Test
    public void testDataBufferCreateSlice2() {
        testCreateSlice2(true);
    }

    @Test
    public void testEventBufferCreateSlice2() {
        testCreateSlice2(false);
    }

    private static void testCreateSlice2(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(1024, 1024, isBuffer);
        buffer.setSize(2); // fake some data
        ReadOnlySlicedNetworkBuffer slice = buffer.readOnlySlice(1, 10);

        assertThat(slice.getReaderIndex()).isEqualTo(0);
        assertThat(slice.getSize()).isEqualTo(10);
        assertThat(slice.unwrap().unwrap()).isSameAs(buffer);

        // slice indices should be independent:
        buffer.setSize(8);
        buffer.setReaderIndex(2);
        assertThat(slice.getReaderIndex()).isEqualTo(0);
        assertThat(slice.getSize()).isEqualTo(10);
    }

    @Test
    public void testDataBufferGetMaxCapacity() {
        testGetMaxCapacity(true);
    }

    @Test
    public void testEventBufferGetMaxCapacity() {
        testGetMaxCapacity(false);
    }

    private static void testGetMaxCapacity(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(100, 1024, isBuffer);
        assertThat(buffer.getMaxCapacity()).isEqualTo(1024);
        MemorySegment segment = buffer.getMemorySegment();
        assertThat(buffer.getMaxCapacity()).isEqualTo(segment.size());
        assertThat(buffer.maxCapacity()).isEqualTo(segment.size());
    }

    @Test
    public void testDataBufferGetSetReaderIndex() {
        testGetSetReaderIndex(true);
    }

    @Test
    public void testEventBufferGetSetReaderIndex() {
        testGetSetReaderIndex(false);
    }

    /**
     * Tests that {@link NetworkBuffer#setReaderIndex(int)} and {@link
     * NetworkBuffer#getReaderIndex()} are consistent.
     */
    private static void testGetSetReaderIndex(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(100, 1024, isBuffer);
        assertThat(buffer.getReaderIndex()).isEqualTo(0);

        // fake some data
        buffer.setSize(100);
        assertThat(buffer.getReaderIndex()).isEqualTo(0);
        buffer.setReaderIndex(1);
        assertThat(buffer.getReaderIndex()).isEqualTo(1);
    }

    @Test
    public void testDataBufferSetGetSize() {
        testSetGetSize(true);
    }

    @Test
    public void testEventBufferSetGetSize() {
        testSetGetSize(false);
    }

    private static void testSetGetSize(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(1024, 1024, isBuffer);

        assertThat(buffer.getSize()).isEqualTo(0); // initially 0
        assertThat(buffer.getSize()).isEqualTo(buffer.writerIndex());
        assertThat(buffer.readerIndex()).isEqualTo(0); // initially 0

        buffer.setSize(10);
        assertThat(buffer.getSize()).isEqualTo(10);
        assertThat(buffer.getSize()).isEqualTo(buffer.writerIndex());
        assertThat(buffer.readerIndex()).isEqualTo(0); // independent
    }

    @Test
    public void testDataBufferReadableBytes() {
        testReadableBytes(true);
    }

    @Test
    public void testEventBufferReadableBytes() {
        testReadableBytes(false);
    }

    private static void testReadableBytes(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(1024, 1024, isBuffer);

        assertThat(buffer.readableBytes()).isEqualTo(0);
        buffer.setSize(10);
        assertThat(buffer.readableBytes()).isEqualTo(10);
        buffer.setReaderIndex(2);
        assertThat(buffer.readableBytes()).isEqualTo(8);
        buffer.setReaderIndex(10);
        assertThat(buffer.readableBytes()).isEqualTo(0);
    }

    @Test
    public void testDataBufferGetNioBufferReadable() {
        testGetNioBufferReadable(true);
    }

    @Test
    public void testEventBufferGetNioBufferReadable() {
        testGetNioBufferReadable(false);
    }

    private void testGetNioBufferReadable(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(1024, 1024, isBuffer);

        ByteBuffer byteBuffer = buffer.getNioBufferReadable();
        assertThat(byteBuffer.isReadOnly()).isFalse();
        assertThat(byteBuffer.remaining()).isEqualTo(0);
        assertThat(byteBuffer.limit()).isEqualTo(0);
        assertThat(byteBuffer.capacity()).isEqualTo(0);

        // add some data
        buffer.setSize(10);
        // nothing changes in the byteBuffer
        assertThat(byteBuffer.remaining()).isEqualTo(0);
        assertThat(byteBuffer.limit()).isEqualTo(0);
        assertThat(byteBuffer.capacity()).isEqualTo(0);
        // get a new byteBuffer (should have updated indices)
        byteBuffer = buffer.getNioBufferReadable();
        assertThat(byteBuffer.isReadOnly()).isFalse();
        assertThat(byteBuffer.remaining()).isEqualTo(10);
        assertThat(byteBuffer.limit()).isEqualTo(10);
        assertThat(byteBuffer.capacity()).isEqualTo(10);

        // modify byteBuffer position and verify nothing has changed in the original buffer
        byteBuffer.position(1);
        assertThat(buffer.getReaderIndex()).isEqualTo(0);
        assertThat(buffer.getSize()).isEqualTo(10);
    }

    @Test
    public void testGetNioBufferReadableThreadSafe() {
        NetworkBuffer buffer = newBuffer(1024, 1024);
        testGetNioBufferReadableThreadSafe(buffer);
    }

    static void testGetNioBufferReadableThreadSafe(Buffer buffer) {
        ByteBuffer buf1 = buffer.getNioBufferReadable();
        ByteBuffer buf2 = buffer.getNioBufferReadable();

        assertThat(buf1).isNotNull();
        assertThat(buf2).isNotNull();

        assertThat(buf1 != buf2)
                .as("Repeated call to getNioBuffer() returns the same nio buffer")
                .isTrue();
    }

    @Test
    public void testDataBufferGetNioBuffer() {
        testGetNioBuffer(true);
    }

    @Test
    public void testEventBufferGetNioBuffer() {
        testGetNioBuffer(false);
    }

    private void testGetNioBuffer(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(1024, 1024, isBuffer);

        ByteBuffer byteBuffer = buffer.getNioBuffer(1, 1);
        assertThat(byteBuffer.isReadOnly()).isFalse();
        assertThat(byteBuffer.remaining()).isEqualTo(1);
        assertThat(byteBuffer.limit()).isEqualTo(1);
        assertThat(byteBuffer.capacity()).isEqualTo(1);

        // add some data
        buffer.setSize(10);
        // nothing changes in the byteBuffer
        assertThat(byteBuffer.remaining()).isEqualTo(1);
        assertThat(byteBuffer.limit()).isEqualTo(1);
        assertThat(byteBuffer.capacity()).isEqualTo(1);
        // get a new byteBuffer (should have updated indices)
        byteBuffer = buffer.getNioBuffer(1, 2);
        assertThat(byteBuffer.isReadOnly()).isFalse();
        assertThat(byteBuffer.remaining()).isEqualTo(2);
        assertThat(byteBuffer.limit()).isEqualTo(2);
        assertThat(byteBuffer.capacity()).isEqualTo(2);

        // modify byteBuffer position and verify nothing has changed in the original buffer
        byteBuffer.position(1);
        assertThat(buffer.getReaderIndex()).isEqualTo(0);
        assertThat(buffer.getSize()).isEqualTo(10);
    }

    @Test
    public void testGetNioBufferThreadSafe() {
        NetworkBuffer buffer = newBuffer(1024, 1024);
        testGetNioBufferThreadSafe(buffer, 10);
    }

    static void testGetNioBufferThreadSafe(Buffer buffer, int length) {
        ByteBuffer buf1 = buffer.getNioBuffer(0, length);
        ByteBuffer buf2 = buffer.getNioBuffer(0, length);

        assertThat(buf1).isNotNull();
        assertThat(buf2).isNotNull();

        assertThat(buf1 != buf2)
                .as("Repeated call to getNioBuffer(int, int) returns the same nio buffer")
                .isTrue();
    }

    @Test
    public void testDataBufferSetAllocator() {
        testSetAllocator(true);
    }

    @Test
    public void testEventBufferSetAllocator() {
        testSetAllocator(false);
    }

    private void testSetAllocator(boolean isBuffer) {
        NetworkBuffer buffer = newBuffer(1024, 1024, isBuffer);
        NettyBufferPool allocator = new NettyBufferPool(1);

        buffer.setAllocator(allocator);
        assertThat(buffer.alloc()).isSameAs(allocator);
    }
}
