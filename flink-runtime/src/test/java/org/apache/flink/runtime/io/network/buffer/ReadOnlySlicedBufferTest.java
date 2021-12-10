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
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.netty.NettyBufferPool;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ReadOnlySlicedNetworkBuffer}. */
public class ReadOnlySlicedBufferTest {
    private static final int BUFFER_SIZE = 1024;
    private static final int DATA_SIZE = 10;

    private NetworkBuffer buffer;

    @Before
    public void setUp() throws Exception {
        final MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(BUFFER_SIZE);
        buffer =
                new NetworkBuffer(
                        segment, FreeingBufferRecycler.INSTANCE, Buffer.DataType.DATA_BUFFER, 0);
        for (int i = 0; i < DATA_SIZE; ++i) {
            buffer.writeByte(i);
        }
    }

    @Test
    public void testForwardsIsBuffer() throws IOException {
        assertThat(buffer.readOnlySlice().isBuffer()).isEqualTo(buffer.isBuffer());
        assertThat(buffer.readOnlySlice(1, 2).isBuffer()).isEqualTo(buffer.isBuffer());
        Buffer eventBuffer = EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE, false);
        assertThat(eventBuffer.readOnlySlice().isBuffer()).isEqualTo(eventBuffer.isBuffer());
        assertThat(eventBuffer.readOnlySlice(1, 2).isBuffer()).isEqualTo(eventBuffer.isBuffer());
    }

    @Test(expected = ReadOnlyBufferException.class)
    public void testSetDataTypeThrows1() {
        buffer.readOnlySlice().setDataType(Buffer.DataType.EVENT_BUFFER);
    }

    @Test(expected = ReadOnlyBufferException.class)
    public void testSetDataTypeThrows2() {
        buffer.readOnlySlice(1, 2).setDataType(Buffer.DataType.EVENT_BUFFER);
    }

    @Test
    public void testForwardsGetMemorySegment() {
        assertThat(buffer.readOnlySlice().getMemorySegment()).isSameAs(buffer.getMemorySegment());
        assertThat(buffer.readOnlySlice(1, 2).getMemorySegment())
                .isSameAs(buffer.getMemorySegment());
    }

    @Test
    public void testForwardsGetRecycler() {
        assertThat(buffer.readOnlySlice().getRecycler()).isSameAs(buffer.getRecycler());
        assertThat(buffer.readOnlySlice(1, 2).getRecycler()).isSameAs(buffer.getRecycler());
    }

    /**
     * Tests forwarding of both {@link ReadOnlySlicedNetworkBuffer#recycleBuffer()} and {@link
     * ReadOnlySlicedNetworkBuffer#isRecycled()}.
     */
    @Test
    public void testForwardsRecycleBuffer1() {
        ReadOnlySlicedNetworkBuffer slice = buffer.readOnlySlice();
        assertThat(slice.isRecycled()).isFalse();
        slice.recycleBuffer();
        assertThat(slice.isRecycled()).isTrue();
        assertThat(buffer.isRecycled()).isTrue();
    }

    /**
     * Tests forwarding of both {@link ReadOnlySlicedNetworkBuffer#recycleBuffer()} and {@link
     * ReadOnlySlicedNetworkBuffer#isRecycled()}.
     */
    @Test
    public void testForwardsRecycleBuffer2() {
        ReadOnlySlicedNetworkBuffer slice = buffer.readOnlySlice(1, 2);
        assertThat(slice.isRecycled()).isFalse();
        slice.recycleBuffer();
        assertThat(slice.isRecycled()).isTrue();
        assertThat(buffer.isRecycled()).isTrue();
    }

    /**
     * Tests forwarding of both {@link ReadOnlySlicedNetworkBuffer#recycleBuffer()} and {@link
     * ReadOnlySlicedNetworkBuffer#isRecycled()}.
     */
    @Test
    public void testForwardsRetainBuffer1() {
        ReadOnlySlicedNetworkBuffer slice = buffer.readOnlySlice();
        assertThat(slice.refCnt()).isEqualTo(buffer.refCnt());
        slice.retainBuffer();
        assertThat(slice.refCnt()).isEqualTo(buffer.refCnt());
    }

    /**
     * Tests forwarding of both {@link ReadOnlySlicedNetworkBuffer#retainBuffer()} and {@link
     * ReadOnlySlicedNetworkBuffer#isRecycled()}.
     */
    @Test
    public void testForwardsRetainBuffer2() {
        ReadOnlySlicedNetworkBuffer slice = buffer.readOnlySlice(1, 2);
        assertThat(slice.refCnt()).isEqualTo(buffer.refCnt());
        slice.retainBuffer();
        assertThat(slice.refCnt()).isEqualTo(buffer.refCnt());
    }

    @Test
    public void testCreateSlice1() {
        buffer.readByte(); // so that we do not start at position 0
        ReadOnlySlicedNetworkBuffer slice1 = buffer.readOnlySlice();
        buffer.readByte(); // should not influence the second slice at all
        ReadOnlySlicedNetworkBuffer slice2 = slice1.readOnlySlice();
        assertThat(slice2.unwrap().unwrap()).isSameAs(buffer);
        assertThat(slice2.getMemorySegment()).isSameAs(slice1.getMemorySegment());
        assertThat(slice1.getMemorySegmentOffset()).isEqualTo(1);
        assertThat(slice2.getMemorySegmentOffset()).isEqualTo(slice1.getMemorySegmentOffset());

        assertReadableBytes(slice1, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        assertReadableBytes(slice2, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    public void testCreateSlice2() {
        buffer.readByte(); // so that we do not start at position 0
        ReadOnlySlicedNetworkBuffer slice1 = buffer.readOnlySlice();
        buffer.readByte(); // should not influence the second slice at all
        ReadOnlySlicedNetworkBuffer slice2 = slice1.readOnlySlice(1, 2);
        assertThat(slice2.unwrap().unwrap()).isSameAs(buffer);
        assertThat(slice2.getMemorySegment()).isSameAs(slice1.getMemorySegment());
        assertThat(slice1.getMemorySegmentOffset()).isEqualTo(1);
        assertThat(slice2.getMemorySegmentOffset()).isEqualTo(2);

        assertReadableBytes(slice1, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        assertReadableBytes(slice2, 2, 3);
    }

    @Test
    public void testCreateSlice3() {
        ReadOnlySlicedNetworkBuffer slice1 = buffer.readOnlySlice(1, 2);
        buffer.readByte(); // should not influence the second slice at all
        ReadOnlySlicedNetworkBuffer slice2 = slice1.readOnlySlice();
        assertThat(slice2.unwrap().unwrap()).isSameAs(buffer);
        assertThat(slice2.getMemorySegment()).isSameAs(slice1.getMemorySegment());
        assertThat(slice1.getMemorySegmentOffset()).isEqualTo(1);
        assertThat(slice2.getMemorySegmentOffset()).isEqualTo(1);

        assertReadableBytes(slice1, 1, 2);
        assertReadableBytes(slice2, 1, 2);
    }

    @Test
    public void testCreateSlice4() {
        ReadOnlySlicedNetworkBuffer slice1 = buffer.readOnlySlice(1, 5);
        buffer.readByte(); // should not influence the second slice at all
        ReadOnlySlicedNetworkBuffer slice2 = slice1.readOnlySlice(1, 2);
        assertThat(slice2.unwrap().unwrap()).isSameAs(buffer);
        assertThat(slice2.getMemorySegment()).isSameAs(slice1.getMemorySegment());
        assertThat(slice1.getMemorySegmentOffset()).isEqualTo(1);
        assertThat(slice2.getMemorySegmentOffset()).isEqualTo(2);

        assertReadableBytes(slice1, 1, 2, 3, 4, 5);
        assertReadableBytes(slice2, 2, 3);
    }

    @Test
    public void testGetMaxCapacity() {
        assertThat(buffer.readOnlySlice().getMaxCapacity()).isEqualTo(DATA_SIZE);
        assertThat(buffer.readOnlySlice(1, 2).getMaxCapacity()).isEqualTo(2);
    }

    /**
     * Tests the independence of the reader index via {@link
     * ReadOnlySlicedNetworkBuffer#setReaderIndex(int)} and {@link
     * ReadOnlySlicedNetworkBuffer#getReaderIndex()}.
     */
    @Test
    public void testGetSetReaderIndex1() {
        testGetSetReaderIndex(buffer.readOnlySlice());
    }

    /**
     * Tests the independence of the reader index via {@link
     * ReadOnlySlicedNetworkBuffer#setReaderIndex(int)} and {@link
     * ReadOnlySlicedNetworkBuffer#getReaderIndex()}.
     */
    @Test
    public void testGetSetReaderIndex2() {
        testGetSetReaderIndex(buffer.readOnlySlice(1, 2));
    }

    private void testGetSetReaderIndex(ReadOnlySlicedNetworkBuffer slice) {
        assertThat(buffer.getReaderIndex()).isEqualTo(0);
        assertThat(slice.getReaderIndex()).isEqualTo(0);
        slice.setReaderIndex(1);
        assertThat(buffer.getReaderIndex()).isEqualTo(0);
        assertThat(slice.getReaderIndex()).isEqualTo(1);
    }

    /**
     * Tests the independence of the writer index via {@link
     * ReadOnlySlicedNetworkBuffer#setSize(int)}, {@link ReadOnlySlicedNetworkBuffer#getSize()}.
     */
    @Test
    public void testGetSetSize1() {
        testGetSetSize(buffer.readOnlySlice(), DATA_SIZE);
    }

    /**
     * Tests the independence of the writer index via {@link
     * ReadOnlySlicedNetworkBuffer#setSize(int)}, {@link ReadOnlySlicedNetworkBuffer#getSize()}.
     */
    @Test
    public void testGetSetSize2() {
        testGetSetSize(buffer.readOnlySlice(1, 2), 2);
    }

    private void testGetSetSize(ReadOnlySlicedNetworkBuffer slice, int sliceSize) {
        assertThat(buffer.getSize()).isEqualTo(DATA_SIZE);
        assertThat(slice.getSize()).isEqualTo(sliceSize);
        buffer.setSize(DATA_SIZE + 1);
        assertThat(buffer.getSize()).isEqualTo(DATA_SIZE + 1);
        assertThat(slice.getSize()).isEqualTo(sliceSize);
    }

    @Test
    public void testReadableBytes() {
        assertThat(buffer.readOnlySlice().readableBytes()).isEqualTo(buffer.readableBytes());
        assertThat(buffer.readOnlySlice(1, 2).readableBytes()).isEqualTo(2);
    }

    @Test
    public void testGetNioBufferReadable1() {
        testGetNioBufferReadable(buffer.readOnlySlice(), DATA_SIZE);
    }

    @Test
    public void testGetNioBufferReadable2() {
        testGetNioBufferReadable(buffer.readOnlySlice(1, 2), 2);
    }

    private void testGetNioBufferReadable(ReadOnlySlicedNetworkBuffer slice, int sliceSize) {
        ByteBuffer sliceByteBuffer = slice.getNioBufferReadable();
        assertThat(sliceByteBuffer.isReadOnly()).isTrue();
        assertThat(sliceByteBuffer.remaining()).isEqualTo(sliceSize);
        assertThat(sliceByteBuffer.limit()).isEqualTo(sliceSize);
        assertThat(sliceByteBuffer.capacity()).isEqualTo(sliceSize);

        // modify sliceByteBuffer position and verify nothing has changed in the original buffer
        sliceByteBuffer.position(1);
        assertThat(buffer.getReaderIndex()).isEqualTo(0);
        assertThat(slice.getReaderIndex()).isEqualTo(0);
        assertThat(buffer.getSize()).isEqualTo(DATA_SIZE);
        assertThat(slice.getSize()).isEqualTo(sliceSize);
    }

    @Test
    public void testGetNioBuffer1() {
        testGetNioBuffer(buffer.readOnlySlice(), DATA_SIZE);
    }

    @Test
    public void testGetNioBuffer2() {
        testGetNioBuffer(buffer.readOnlySlice(1, 2), 2);
    }

    private void testGetNioBuffer(ReadOnlySlicedNetworkBuffer slice, int sliceSize) {
        ByteBuffer sliceByteBuffer = slice.getNioBuffer(1, 1);
        assertThat(sliceByteBuffer.isReadOnly()).isTrue();
        assertThat(sliceByteBuffer.remaining()).isEqualTo(1);
        assertThat(sliceByteBuffer.limit()).isEqualTo(1);
        assertThat(sliceByteBuffer.capacity()).isEqualTo(1);

        // modify sliceByteBuffer position and verify nothing has changed in the original buffer
        sliceByteBuffer.position(1);
        assertThat(buffer.getReaderIndex()).isEqualTo(0);
        assertThat(slice.getReaderIndex()).isEqualTo(0);
        assertThat(buffer.getSize()).isEqualTo(DATA_SIZE);
        assertThat(slice.getSize()).isEqualTo(sliceSize);
    }

    @Test
    public void testGetNioBufferReadableThreadSafe1() {
        NetworkBufferTest.testGetNioBufferReadableThreadSafe(buffer.readOnlySlice());
    }

    @Test
    public void testGetNioBufferReadableThreadSafe2() {
        NetworkBufferTest.testGetNioBufferReadableThreadSafe(buffer.readOnlySlice(1, 2));
    }

    @Test
    public void testGetNioBufferThreadSafe1() {
        NetworkBufferTest.testGetNioBufferThreadSafe(buffer.readOnlySlice(), DATA_SIZE);
    }

    @Test
    public void testGetNioBufferThreadSafe2() {
        NetworkBufferTest.testGetNioBufferThreadSafe(buffer.readOnlySlice(1, 2), 2);
    }

    @Test
    public void testForwardsSetAllocator() {
        testForwardsSetAllocator(buffer.readOnlySlice());
        testForwardsSetAllocator(buffer.readOnlySlice(1, 2));
    }

    private void testForwardsSetAllocator(ReadOnlySlicedNetworkBuffer slice) {
        NettyBufferPool allocator = new NettyBufferPool(1);
        slice.setAllocator(allocator);
        assertThat(slice.alloc()).isSameAs(buffer.alloc());
        assertThat(slice.alloc()).isSameAs(allocator);
    }

    private static void assertReadableBytes(Buffer actualBuffer, int... expectedBytes) {
        ByteBuffer actualBytesBuffer = actualBuffer.getNioBufferReadable();
        int[] actual = new int[actualBytesBuffer.limit()];
        for (int i = 0; i < actual.length; ++i) {
            actual[i] = actualBytesBuffer.get();
        }
        assertThat(actual).isEqualTo(expectedBytes);

        // verify absolutely positioned read method:
        ByteBuf buffer = (ByteBuf) actualBuffer;
        for (int i = 0; i < buffer.readableBytes(); ++i) {
            actual[i] = buffer.getByte(buffer.readerIndex() + i);
        }
        assertThat(actual).isEqualTo(expectedBytes);

        // verify relatively positioned read method:
        for (int i = 0; i < buffer.readableBytes(); ++i) {
            actual[i] = buffer.readByte();
        }
        assertThat(actual).isEqualTo(expectedBytes);
    }
}
