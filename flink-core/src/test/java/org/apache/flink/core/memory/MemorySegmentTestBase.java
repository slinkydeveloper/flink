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

package org.apache.flink.core.memory;

import org.junit.Test;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ReadOnlyBufferException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for the access and transfer methods of the {@link MemorySegment}. */
public abstract class MemorySegmentTestBase {

    private final Random random = new Random();

    private final int pageSize;

    MemorySegmentTestBase(int pageSize) {
        this.pageSize = pageSize;
    }

    // ------------------------------------------------------------------------
    //  Access to primitives
    // ------------------------------------------------------------------------

    abstract MemorySegment createSegment(int size);

    abstract MemorySegment createSegment(
            @SuppressWarnings("SameParameterValue") int size, Object owner);

    // ------------------------------------------------------------------------
    //  Access to primitives
    // ------------------------------------------------------------------------

    @Test
    public void testByteAccess() {
        final MemorySegment segment = createSegment(pageSize);

        // test exceptions
        try {
            segment.put(-1, (byte) 0);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.put(pageSize, (byte) 0);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.put(Integer.MAX_VALUE, (byte) 0);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.put(Integer.MIN_VALUE, (byte) 0);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.get(-1);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.get(pageSize);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.get(Integer.MAX_VALUE);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.get(Integer.MIN_VALUE);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        // test expected correct behavior, sequential access

        long seed = random.nextLong();

        random.setSeed(seed);
        for (int i = 0; i < pageSize; i++) {
            segment.put(i, (byte) random.nextInt());
        }

        random.setSeed(seed);
        for (int i = 0; i < pageSize; i++) {
            assertThat(segment.get(i)).isEqualTo((byte) random.nextInt());
        }

        // test expected correct behavior, random access

        random.setSeed(seed);
        boolean[] occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize);

            if (occupied[pos]) {
                continue;
            } else {
                occupied[pos] = true;
            }

            segment.put(pos, (byte) random.nextInt());
        }

        random.setSeed(seed);
        occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize);

            if (occupied[pos]) {
                continue;
            } else {
                occupied[pos] = true;
            }

            assertThat(segment.get(pos)).isEqualTo((byte) random.nextInt());
        }
    }

    @Test
    public void testBooleanAccess() {
        final MemorySegment segment = createSegment(pageSize);

        // test exceptions
        try {
            segment.putBoolean(-1, false);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.putBoolean(pageSize, false);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.putBoolean(Integer.MAX_VALUE, false);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.putBoolean(Integer.MIN_VALUE, false);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getBoolean(-1);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getBoolean(pageSize);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getBoolean(Integer.MAX_VALUE);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getBoolean(Integer.MIN_VALUE);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        // test expected correct behavior, sequential access

        long seed = random.nextLong();

        random.setSeed(seed);
        for (int i = 0; i < pageSize; i++) {
            segment.putBoolean(i, random.nextBoolean());
        }

        random.setSeed(seed);
        for (int i = 0; i < pageSize; i++) {
            assertThat(segment.getBoolean(i)).isEqualTo(random.nextBoolean());
        }

        // test expected correct behavior, random access

        random.setSeed(seed);
        boolean[] occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize);

            if (occupied[pos]) {
                continue;
            } else {
                occupied[pos] = true;
            }

            segment.putBoolean(pos, random.nextBoolean());
        }

        random.setSeed(seed);
        occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize);

            if (occupied[pos]) {
                continue;
            } else {
                occupied[pos] = true;
            }

            assertThat(segment.getBoolean(pos)).isEqualTo(random.nextBoolean());
        }
    }

    @Test
    public void testCopyUnsafeIndexOutOfBounds() {
        byte[] bytes = new byte[pageSize];
        MemorySegment segment = createSegment(pageSize);

        try {
            segment.copyToUnsafe(1, bytes, 0, pageSize);
            fail("should fail with an IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException ignored) {
        }

        try {
            segment.copyFromUnsafe(1, bytes, 0, pageSize);
            fail("should fail with an IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException ignored) {
        }
    }

    @Test
    public void testEqualTo() {
        MemorySegment seg1 = createSegment(pageSize);
        MemorySegment seg2 = createSegment(pageSize);

        byte[] referenceArray = new byte[pageSize];
        seg1.put(0, referenceArray);
        seg2.put(0, referenceArray);

        int i = new Random().nextInt(pageSize - 8);

        seg1.put(i, (byte) 10);
        assertThat(seg1.equalTo(seg2, i, i, 9)).isFalse();

        seg1.put(i, (byte) 0);
        assertThat(seg1.equalTo(seg2, i, i, 9)).isTrue();

        seg1.put(i + 8, (byte) 10);
        assertThat(seg1.equalTo(seg2, i, i, 9)).isFalse();
    }

    @Test
    public void testCharAccess() {
        final MemorySegment segment = createSegment(pageSize);

        // test exceptions

        try {
            segment.putChar(-1, 'a');
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.putChar(pageSize, 'a');
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.putChar(Integer.MIN_VALUE, 'a');
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.putChar(Integer.MAX_VALUE, 'a');
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.putChar(Integer.MAX_VALUE - 1, 'a');
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getChar(-1);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getChar(pageSize);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getChar(Integer.MIN_VALUE);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getChar(Integer.MAX_VALUE);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getChar(Integer.MAX_VALUE - 1);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        // test expected correct behavior, sequential access

        long seed = random.nextLong();

        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 2; i += 2) {
            segment.putChar(i, (char) (random.nextInt(Character.MAX_VALUE)));
        }

        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 2; i += 2) {
            assertThat(segment.getChar(i)).isEqualTo((char) (random.nextInt(Character.MAX_VALUE)));
        }

        // test expected correct behavior, random access

        random.setSeed(seed);
        boolean[] occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 1);

            if (occupied[pos] || occupied[pos + 1]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
            }

            segment.putChar(pos, (char) (random.nextInt(Character.MAX_VALUE)));
        }

        random.setSeed(seed);
        occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 1);

            if (occupied[pos] || occupied[pos + 1]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
            }

            assertThat(segment.getChar(pos))
                    .isEqualTo((char) (random.nextInt(Character.MAX_VALUE)));
        }
    }

    @Test
    public void testShortAccess() {
        final MemorySegment segment = createSegment(pageSize);

        // test exceptions

        try {
            segment.putShort(-1, (short) 0);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.putShort(pageSize, (short) 0);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.putShort(Integer.MIN_VALUE, (short) 0);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.putShort(Integer.MAX_VALUE, (short) 0);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.putShort(Integer.MAX_VALUE - 1, (short) 0);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getShort(-1);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getShort(pageSize);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getShort(Integer.MIN_VALUE);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getShort(Integer.MAX_VALUE);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getShort(Integer.MAX_VALUE - 1);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        // test expected correct behavior, sequential access

        long seed = random.nextLong();

        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 2; i += 2) {
            segment.putShort(i, (short) random.nextInt());
        }

        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 2; i += 2) {
            assertThat(segment.getShort(i)).isEqualTo((short) random.nextInt());
        }

        // test expected correct behavior, random access

        random.setSeed(seed);
        boolean[] occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 1);

            if (occupied[pos] || occupied[pos + 1]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
            }

            segment.putShort(pos, (short) random.nextInt());
        }

        random.setSeed(seed);
        occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 1);

            if (occupied[pos] || occupied[pos + 1]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
            }

            assertThat(segment.getShort(pos)).isEqualTo((short) random.nextInt());
        }
    }

    @Test
    public void testIntAccess() {
        final MemorySegment segment = createSegment(pageSize);

        // test exceptions

        try {
            segment.putInt(-1, 0);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.putInt(pageSize, 0);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.putInt(pageSize - 3, 0);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.putInt(Integer.MIN_VALUE, 0);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.putInt(Integer.MAX_VALUE, 0);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.putInt(Integer.MAX_VALUE - 3, 0);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getInt(-1);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getInt(pageSize);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getInt(pageSize - 3);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getInt(Integer.MIN_VALUE);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getInt(Integer.MAX_VALUE);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getInt(Integer.MAX_VALUE - 3);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        // test expected correct behavior, sequential access

        long seed = random.nextLong();

        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 4; i += 4) {
            segment.putInt(i, random.nextInt());
        }

        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 4; i += 4) {
            assertThat(segment.getInt(i)).isEqualTo(random.nextInt());
        }

        // test expected correct behavior, random access

        random.setSeed(seed);
        boolean[] occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 3);

            if (occupied[pos] || occupied[pos + 1] || occupied[pos + 2] || occupied[pos + 3]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
                occupied[pos + 2] = true;
                occupied[pos + 3] = true;
            }

            segment.putInt(pos, random.nextInt());
        }

        random.setSeed(seed);
        occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 3);

            if (occupied[pos] || occupied[pos + 1] || occupied[pos + 2] || occupied[pos + 3]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
                occupied[pos + 2] = true;
                occupied[pos + 3] = true;
            }

            assertThat(segment.getInt(pos)).isEqualTo(random.nextInt());
        }
    }

    @Test
    public void testLongAccess() {
        final MemorySegment segment = createSegment(pageSize);

        // test exceptions

        try {
            segment.putLong(-1, 0L);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.putLong(pageSize, 0L);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.putLong(pageSize - 7, 0L);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.putLong(Integer.MIN_VALUE, 0L);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.putLong(Integer.MAX_VALUE, 0L);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.putLong(Integer.MAX_VALUE - 7, 0L);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getLong(-1);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getLong(pageSize);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getLong(pageSize - 7);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getLong(Integer.MIN_VALUE);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getLong(Integer.MAX_VALUE);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getLong(Integer.MAX_VALUE - 7);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        // test expected correct behavior, sequential access

        long seed = random.nextLong();

        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 8; i += 8) {
            segment.putLong(i, random.nextLong());
        }

        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 8; i += 8) {
            assertThat(segment.getLong(i)).isEqualTo(random.nextLong());
        }

        // test expected correct behavior, random access

        random.setSeed(seed);
        boolean[] occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 7);

            if (occupied[pos]
                    || occupied[pos + 1]
                    || occupied[pos + 2]
                    || occupied[pos + 3]
                    || occupied[pos + 4]
                    || occupied[pos + 5]
                    || occupied[pos + 6]
                    || occupied[pos + 7]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
                occupied[pos + 2] = true;
                occupied[pos + 3] = true;
                occupied[pos + 4] = true;
                occupied[pos + 5] = true;
                occupied[pos + 6] = true;
                occupied[pos + 7] = true;
            }

            segment.putLong(pos, random.nextLong());
        }

        random.setSeed(seed);
        occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 7);

            if (occupied[pos]
                    || occupied[pos + 1]
                    || occupied[pos + 2]
                    || occupied[pos + 3]
                    || occupied[pos + 4]
                    || occupied[pos + 5]
                    || occupied[pos + 6]
                    || occupied[pos + 7]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
                occupied[pos + 2] = true;
                occupied[pos + 3] = true;
                occupied[pos + 4] = true;
                occupied[pos + 5] = true;
                occupied[pos + 6] = true;
                occupied[pos + 7] = true;
            }

            assertThat(segment.getLong(pos)).isEqualTo(random.nextLong());
        }
    }

    @Test
    public void testFloatAccess() {
        final MemorySegment segment = createSegment(pageSize);

        // test exceptions

        try {
            segment.putFloat(-1, 0.0f);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.putFloat(pageSize, 0.0f);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.putFloat(pageSize - 3, 0.0f);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.putFloat(Integer.MIN_VALUE, 0.0f);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.putFloat(Integer.MAX_VALUE, 0.0f);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.putFloat(Integer.MAX_VALUE - 3, 0.0f);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getFloat(-1);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getFloat(pageSize);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getFloat(pageSize - 3);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getFloat(Integer.MIN_VALUE);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getFloat(Integer.MAX_VALUE);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getFloat(Integer.MAX_VALUE - 3);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        // test expected correct behavior, sequential access

        long seed = random.nextLong();

        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 4; i += 4) {
            segment.putFloat(i, random.nextFloat());
        }

        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 4; i += 4) {
            assertThat(segment.getFloat(i)).isEqualTo(random.nextFloat());
        }

        // test expected correct behavior, random access

        random.setSeed(seed);
        boolean[] occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 3);

            if (occupied[pos] || occupied[pos + 1] || occupied[pos + 2] || occupied[pos + 3]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
                occupied[pos + 2] = true;
                occupied[pos + 3] = true;
            }

            segment.putFloat(pos, random.nextFloat());
        }

        random.setSeed(seed);
        occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 3);

            if (occupied[pos] || occupied[pos + 1] || occupied[pos + 2] || occupied[pos + 3]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
                occupied[pos + 2] = true;
                occupied[pos + 3] = true;
            }

            assertThat(segment.getFloat(pos)).isEqualTo(random.nextFloat());
        }
    }

    @Test
    public void testDoubleAccess() {
        final MemorySegment segment = createSegment(pageSize);

        // test exceptions
        try {
            segment.putDouble(-1, 0.0);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.putDouble(pageSize, 0.0);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.putDouble(pageSize - 7, 0.0);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.putDouble(Integer.MIN_VALUE, 0.0);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.putDouble(Integer.MAX_VALUE, 0.0);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.putDouble(Integer.MAX_VALUE - 7, 0.0);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getDouble(-1);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getDouble(pageSize);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getDouble(pageSize - 7);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getDouble(Integer.MIN_VALUE);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getDouble(Integer.MAX_VALUE);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.getDouble(Integer.MAX_VALUE - 7);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        // test expected correct behavior, sequential access

        long seed = random.nextLong();

        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 8; i += 8) {
            segment.putDouble(i, random.nextDouble());
        }

        random.setSeed(seed);
        for (int i = 0; i <= pageSize - 8; i += 8) {
            assertThat(segment.getDouble(i)).isEqualTo(random.nextDouble());
        }

        // test expected correct behavior, random access

        random.setSeed(seed);
        boolean[] occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 7);

            if (occupied[pos]
                    || occupied[pos + 1]
                    || occupied[pos + 2]
                    || occupied[pos + 3]
                    || occupied[pos + 4]
                    || occupied[pos + 5]
                    || occupied[pos + 6]
                    || occupied[pos + 7]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
                occupied[pos + 2] = true;
                occupied[pos + 3] = true;
                occupied[pos + 4] = true;
                occupied[pos + 5] = true;
                occupied[pos + 6] = true;
                occupied[pos + 7] = true;
            }

            segment.putDouble(pos, random.nextDouble());
        }

        random.setSeed(seed);
        occupied = new boolean[pageSize];

        for (int i = 0; i < 1000; i++) {
            int pos = random.nextInt(pageSize - 7);

            if (occupied[pos]
                    || occupied[pos + 1]
                    || occupied[pos + 2]
                    || occupied[pos + 3]
                    || occupied[pos + 4]
                    || occupied[pos + 5]
                    || occupied[pos + 6]
                    || occupied[pos + 7]) {
                continue;
            } else {
                occupied[pos] = true;
                occupied[pos + 1] = true;
                occupied[pos + 2] = true;
                occupied[pos + 3] = true;
                occupied[pos + 4] = true;
                occupied[pos + 5] = true;
                occupied[pos + 6] = true;
                occupied[pos + 7] = true;
            }

            assertThat(segment.getDouble(pos)).isEqualTo(random.nextDouble());
        }
    }

    // ------------------------------------------------------------------------
    //  Bulk Byte Movements
    // ------------------------------------------------------------------------

    @Test
    public void testBulkBytePutExceptions() {
        final MemorySegment segment = createSegment(pageSize);

        byte[] bytes = new byte[pageSize / 4 + (pageSize % 4)];
        random.nextBytes(bytes);

        // wrong positions into memory segment

        try {
            segment.put(-1, bytes);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.put(-1, bytes, 4, 5);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.put(Integer.MIN_VALUE, bytes);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.put(Integer.MIN_VALUE, bytes, 4, 5);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.put(pageSize, bytes);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.put(pageSize, bytes, 6, 44);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.put(pageSize - bytes.length + 1, bytes);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.put(pageSize - 5, bytes, 3, 6);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.put(Integer.MAX_VALUE, bytes);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.put(Integer.MAX_VALUE, bytes, 10, 20);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.put(Integer.MAX_VALUE - bytes.length + 1, bytes);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.put(Integer.MAX_VALUE - 11, bytes, 11, 11);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.put(3 * (pageSize / 4) + 1, bytes);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.put(3 * (pageSize / 4) + 2, bytes, 0, bytes.length - 1);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.put(7 * (pageSize / 8) + 1, bytes, 0, bytes.length / 2);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        // wrong source array positions / lengths

        try {
            segment.put(0, bytes, -1, 1);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.put(0, bytes, -1, bytes.length + 1);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.put(0, bytes, Integer.MIN_VALUE, bytes.length);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.put(0, bytes, Integer.MAX_VALUE, bytes.length);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.put(0, bytes, Integer.MAX_VALUE - bytes.length + 1, bytes.length);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        // case where negative offset and negative index compensate each other
        try {
            segment.put(-2, bytes, -1, bytes.length / 2);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }
    }

    @Test
    public void testBulkByteGetExceptions() {
        final MemorySegment segment = createSegment(pageSize);

        byte[] bytes = new byte[pageSize / 4];

        // wrong positions into memory segment

        try {
            segment.get(-1, bytes);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.get(-1, bytes, 4, 5);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.get(Integer.MIN_VALUE, bytes);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.get(Integer.MIN_VALUE, bytes, 4, 5);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.get(pageSize, bytes);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.get(pageSize, bytes, 6, 44);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.get(pageSize - bytes.length + 1, bytes);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.get(pageSize - 5, bytes, 3, 6);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.get(Integer.MAX_VALUE, bytes);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.get(Integer.MAX_VALUE, bytes, 10, 20);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.get(Integer.MAX_VALUE - bytes.length + 1, bytes);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.get(Integer.MAX_VALUE - 11, bytes, 11, 11);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.get(3 * (pageSize / 4) + 1, bytes);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.get(3 * (pageSize / 4) + 2, bytes, 0, bytes.length - 1);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.get(7 * (pageSize / 8) + 1, bytes, 0, bytes.length / 2);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        // wrong source array positions / lengths

        try {
            segment.get(0, bytes, -1, 1);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.get(0, bytes, -1, bytes.length + 1);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.get(0, bytes, Integer.MIN_VALUE, bytes.length);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.get(0, bytes, Integer.MAX_VALUE, bytes.length);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        try {
            segment.get(0, bytes, Integer.MAX_VALUE - bytes.length + 1, bytes.length);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }

        // case where negative offset and negative index compensate each other
        try {
            segment.get(-2, bytes, -1, bytes.length / 2);
            fail("IndexOutOfBoundsException expected");
        } catch (Exception e) {
            assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
        }
    }

    @Test
    public void testBulkByteAccess() {
        // test expected correct behavior with default offset / length
        {
            final MemorySegment segment = createSegment(pageSize);
            long seed = random.nextLong();

            random.setSeed(seed);
            byte[] src = new byte[pageSize / 8];
            for (int i = 0; i < 8; i++) {
                random.nextBytes(src);
                segment.put(i * (pageSize / 8), src);
            }

            random.setSeed(seed);
            byte[] expected = new byte[pageSize / 8];
            byte[] actual = new byte[pageSize / 8];
            for (int i = 0; i < 8; i++) {
                random.nextBytes(expected);
                segment.get(i * (pageSize / 8), actual);

                assertThat(actual).isEqualTo(expected);
            }
        }

        // test expected correct behavior with specific offset / length
        {
            final MemorySegment segment = createSegment(pageSize);
            byte[] expected = new byte[pageSize];
            random.nextBytes(expected);

            for (int i = 0; i < 16; i++) {
                segment.put(i * (pageSize / 16), expected, i * (pageSize / 16), pageSize / 16);
            }

            byte[] actual = new byte[pageSize];
            for (int i = 0; i < 16; i++) {
                segment.get(i * (pageSize / 16), actual, i * (pageSize / 16), pageSize / 16);
            }

            assertThat(actual).isEqualTo(expected);
        }

        // put segments of various lengths to various positions
        {
            final MemorySegment segment = createSegment(pageSize);
            byte[] expected = new byte[pageSize];
            segment.put(0, expected, 0, pageSize);

            for (int i = 0; i < 200; i++) {
                int numBytes = random.nextInt(pageSize - 10) + 1;
                int pos = random.nextInt(pageSize - numBytes + 1);

                byte[] data = new byte[(random.nextInt(3) + 1) * numBytes];
                int dataStartPos = random.nextInt(data.length - numBytes + 1);

                random.nextBytes(data);

                // copy to the expected
                System.arraycopy(data, dataStartPos, expected, pos, numBytes);

                // put to the memory segment
                segment.put(pos, data, dataStartPos, numBytes);
            }

            byte[] validation = new byte[pageSize];
            segment.get(0, validation);

            assertThat(validation).isEqualTo(expected);
        }

        // get segments with various contents
        {
            final MemorySegment segment = createSegment(pageSize);
            byte[] contents = new byte[pageSize];
            random.nextBytes(contents);
            segment.put(0, contents);

            for (int i = 0; i < 200; i++) {
                int numBytes = random.nextInt(pageSize / 8) + 1;
                int pos = random.nextInt(pageSize - numBytes + 1);

                byte[] data = new byte[(random.nextInt(3) + 1) * numBytes];
                int dataStartPos = random.nextInt(data.length - numBytes + 1);

                segment.get(pos, data, dataStartPos, numBytes);

                byte[] expected = Arrays.copyOfRange(contents, pos, pos + numBytes);
                byte[] validation = Arrays.copyOfRange(data, dataStartPos, dataStartPos + numBytes);
                assertThat(validation).isEqualTo(expected);
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Writing / Reading to/from DataInput / DataOutput
    // ------------------------------------------------------------------------

    @Test
    public void testDataInputOutput() throws IOException {
        MemorySegment seg = createSegment(pageSize);
        byte[] contents = new byte[pageSize];
        random.nextBytes(contents);
        seg.put(0, contents);

        ByteArrayOutputStream buffer = new ByteArrayOutputStream(pageSize);
        DataOutputStream out = new DataOutputStream(buffer);

        // write the segment in chunks into the stream
        int pos = 0;
        while (pos < pageSize) {
            int len = random.nextInt(200);
            len = Math.min(len, pageSize - pos);
            seg.get(out, pos, len);
            pos += len;
        }

        // verify that we wrote the same bytes
        byte[] result = buffer.toByteArray();
        assertThat(result).isEqualTo(contents);

        // re-read the bytes into a new memory segment
        MemorySegment reader = createSegment(pageSize);
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(result));

        pos = 0;
        while (pos < pageSize) {
            int len = random.nextInt(200);
            len = Math.min(len, pageSize - pos);
            reader.put(in, pos, len);
            pos += len;
        }

        byte[] targetBuffer = new byte[pageSize];
        reader.get(0, targetBuffer);

        assertThat(targetBuffer).isEqualTo(contents);
    }

    @Test
    public void testDataInputOutputOutOfBounds() {
        final int segmentSize = 52;

        // segment with random contents
        MemorySegment seg = createSegment(segmentSize);
        byte[] bytes = new byte[segmentSize];
        random.nextBytes(bytes);
        seg.put(0, bytes);

        // out of bounds when writing
        {
            DataOutputStream out = new DataOutputStream(new ByteArrayOutputStream());

            try {
                seg.get(out, -1, segmentSize / 2);
                fail("IndexOutOfBoundsException expected");
            } catch (Exception e) {
                assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
            }

            try {
                seg.get(out, segmentSize, segmentSize / 2);
                fail("IndexOutOfBoundsException expected");
            } catch (Exception e) {
                assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
            }

            try {
                seg.get(out, -segmentSize, segmentSize / 2);
                fail("IndexOutOfBoundsException expected");
            } catch (Exception e) {
                assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
            }

            try {
                seg.get(out, Integer.MIN_VALUE, segmentSize / 2);
                fail("IndexOutOfBoundsException expected");
            } catch (Exception e) {
                assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
            }

            try {
                seg.get(out, Integer.MAX_VALUE, segmentSize / 2);
                fail("IndexOutOfBoundsException expected");
            } catch (Exception e) {
                assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
            }
        }

        // out of bounds when reading
        {
            DataInputStream in =
                    new DataInputStream(new ByteArrayInputStream(new byte[segmentSize]));

            try {
                seg.put(in, -1, segmentSize / 2);
                fail("IndexOutOfBoundsException expected");
            } catch (Exception e) {
                assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
            }

            try {
                seg.put(in, segmentSize, segmentSize / 2);
                fail("IndexOutOfBoundsException expected");
            } catch (Exception e) {
                assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
            }

            try {
                seg.put(in, -segmentSize, segmentSize / 2);
                fail("IndexOutOfBoundsException expected");
            } catch (Exception e) {
                assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
            }

            try {
                seg.put(in, Integer.MIN_VALUE, segmentSize / 2);
                fail("IndexOutOfBoundsException expected");
            } catch (Exception e) {
                assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
            }

            try {
                seg.put(in, Integer.MAX_VALUE, segmentSize / 2);
                fail("IndexOutOfBoundsException expected");
            } catch (Exception e) {
                assertThat(e).isInstanceOf(IndexOutOfBoundsException.class);
            }
        }
    }

    @Test
    public void testDataInputOutputStreamUnderflowOverflow() throws IOException {
        final int segmentSize = 1337;

        // segment with random contents
        MemorySegment seg = createSegment(segmentSize);
        byte[] bytes = new byte[segmentSize];
        random.nextBytes(bytes);
        seg.put(0, bytes);

        // a stream that we cannot fully write to
        DataOutputStream out =
                new DataOutputStream(
                        new OutputStream() {

                            int bytesSoFar = 0;

                            @Override
                            public void write(int b) throws IOException {
                                bytesSoFar++;
                                if (bytesSoFar > segmentSize / 2) {
                                    throw new IOException("overflow");
                                }
                            }
                        });

        // write the segment in chunks into the stream
        try {
            int pos = 0;
            while (pos < pageSize) {
                int len = random.nextInt(segmentSize / 10);
                len = Math.min(len, pageSize - pos);
                seg.get(out, pos, len);
                pos += len;
            }
            fail("Should fail with an IOException");
        } catch (IOException e) {
            // expected
        }

        DataInputStream in =
                new DataInputStream(new ByteArrayInputStream(new byte[segmentSize / 2]));

        try {
            int pos = 0;
            while (pos < pageSize) {
                int len = random.nextInt(segmentSize / 10);
                len = Math.min(len, pageSize - pos);
                seg.put(in, pos, len);
                pos += len;
            }
            fail("Should fail with an EOFException");
        } catch (EOFException e) {
            // expected
        }
    }

    // ------------------------------------------------------------------------
    //  ByteBuffer Ops
    // ------------------------------------------------------------------------

    @Test
    public void testByteBufferGet() {
        testByteBufferGet(false);
        testByteBufferGet(true);
    }

    private void testByteBufferGet(boolean directBuffer) {
        MemorySegment seg = createSegment(pageSize);
        byte[] bytes = new byte[pageSize];
        random.nextBytes(bytes);
        seg.put(0, bytes);

        ByteBuffer target =
                directBuffer
                        ? ByteBuffer.allocateDirect(3 * pageSize)
                        : ByteBuffer.allocate(3 * pageSize);
        target.position(2 * pageSize);

        // transfer the segment in chunks into the byte buffer
        int pos = 0;
        while (pos < pageSize) {
            int len = random.nextInt(pageSize / 10);
            len = Math.min(len, pageSize - pos);
            seg.get(pos, target, len);
            pos += len;
        }

        // verify that we wrote the same bytes
        byte[] result = new byte[pageSize];
        target.position(2 * pageSize);
        target.get(result);

        assertThat(result).isEqualTo(bytes);
    }

    @Test(expected = ReadOnlyBufferException.class)
    public void testHeapByteBufferGetReadOnly() {
        testByteBufferGetReadOnly(false);
    }

    @Test(expected = ReadOnlyBufferException.class)
    public void testOffHeapByteBufferGetReadOnly() {
        testByteBufferGetReadOnly(true);
    }

    /**
     * Tries to write into a {@link ByteBuffer} instance which is read-only. This should fail with a
     * {@link ReadOnlyBufferException}.
     *
     * @param directBuffer whether the {@link ByteBuffer} instance should be a direct byte buffer or
     *     not
     * @throws ReadOnlyBufferException expected exception due to writing to a read-only buffer
     */
    private void testByteBufferGetReadOnly(boolean directBuffer) throws ReadOnlyBufferException {
        MemorySegment seg = createSegment(pageSize);

        ByteBuffer target =
                (directBuffer ? ByteBuffer.allocateDirect(pageSize) : ByteBuffer.allocate(pageSize))
                        .asReadOnlyBuffer();

        seg.get(0, target, pageSize);
    }

    @Test
    public void testByteBufferPut() {
        testByteBufferPut(false);
        testByteBufferPut(true);
    }

    private void testByteBufferPut(boolean directBuffer) {
        byte[] bytes = new byte[pageSize];
        random.nextBytes(bytes);

        ByteBuffer source =
                directBuffer ? ByteBuffer.allocateDirect(pageSize) : ByteBuffer.allocate(pageSize);

        source.put(bytes);
        source.clear();

        MemorySegment seg = createSegment(3 * pageSize);

        int offset = 2 * pageSize;

        // transfer the segment in chunks into the byte buffer
        int pos = 0;
        while (pos < pageSize) {
            int len = random.nextInt(pageSize / 10);
            len = Math.min(len, pageSize - pos);
            seg.put(offset + pos, source, len);
            pos += len;
        }

        // verify that we read the same bytes
        byte[] result = new byte[pageSize];
        seg.get(offset, result);

        assertThat(result).isEqualTo(bytes);
    }

    // ------------------------------------------------------------------------
    //  ByteBuffer Ops on sliced byte buffers
    // ------------------------------------------------------------------------

    @Test
    public void testSlicedByteBufferGet() {
        testSlicedByteBufferGet(false);
        testSlicedByteBufferGet(true);
    }

    private void testSlicedByteBufferGet(boolean directBuffer) {
        MemorySegment seg = createSegment(pageSize);
        byte[] bytes = new byte[pageSize];
        random.nextBytes(bytes);
        seg.put(0, bytes);

        ByteBuffer target =
                directBuffer
                        ? ByteBuffer.allocateDirect(pageSize + 49)
                        : ByteBuffer.allocate(pageSize + 49);

        target.position(19).limit(19 + pageSize);

        ByteBuffer slicedTarget = target.slice();

        // transfer the segment in chunks into the byte buffer
        int pos = 0;
        while (pos < pageSize) {
            int len = random.nextInt(pageSize / 10);
            len = Math.min(len, pageSize - pos);
            seg.get(pos, slicedTarget, len);
            pos += len;
        }

        // verify that we wrote the same bytes
        byte[] result = new byte[pageSize];
        target.position(19);
        target.get(result);

        assertThat(result).isEqualTo(bytes);
    }

    @Test
    public void testSlicedByteBufferPut() {
        testSlicedByteBufferPut(false);
        testSlicedByteBufferPut(true);
    }

    private void testSlicedByteBufferPut(boolean directBuffer) {
        byte[] bytes = new byte[pageSize + 49];
        random.nextBytes(bytes);

        ByteBuffer source =
                directBuffer
                        ? ByteBuffer.allocateDirect(pageSize + 49)
                        : ByteBuffer.allocate(pageSize + 49);

        source.put(bytes);
        source.position(19).limit(19 + pageSize);
        ByteBuffer slicedSource = source.slice();

        MemorySegment seg = createSegment(3 * pageSize);

        final int offset = 2 * pageSize;

        // transfer the segment in chunks into the byte buffer
        int pos = 0;
        while (pos < pageSize) {
            int len = random.nextInt(pageSize / 10);
            len = Math.min(len, pageSize - pos);
            seg.put(offset + pos, slicedSource, len);
            pos += len;
        }

        // verify that we read the same bytes
        byte[] result = new byte[pageSize];
        seg.get(offset, result);

        byte[] expected = Arrays.copyOfRange(bytes, 19, 19 + pageSize);
        assertThat(result).isEqualTo(expected);
    }

    // ------------------------------------------------------------------------
    //  ByteBuffer overflow / underflow and out of bounds
    // ------------------------------------------------------------------------

    @Test
    public void testByteBufferOutOfBounds() {
        final int bbCapacity = pageSize / 10;

        final int[] validOffsets = {0, 1, pageSize / 10 * 9};
        final int[] invalidOffsets = {
            -1, pageSize + 1, -pageSize, Integer.MAX_VALUE, Integer.MIN_VALUE
        };

        final int[] validLengths = {0, 1, bbCapacity, pageSize};
        final int[] invalidLengths = {-1, -pageSize, Integer.MAX_VALUE, Integer.MIN_VALUE};

        final MemorySegment seg = createSegment(pageSize);

        for (ByteBuffer bb :
                new ByteBuffer[] {
                    ByteBuffer.allocate(bbCapacity), ByteBuffer.allocateDirect(bbCapacity)
                }) {
            for (int off : validOffsets) {
                for (int len : invalidLengths) {
                    try {
                        seg.put(off, bb, len);
                        fail("should fail with an IndexOutOfBoundsException");
                    } catch (IndexOutOfBoundsException | BufferUnderflowException ignored) {
                    }

                    try {
                        seg.get(off, bb, len);
                        fail("should fail with an IndexOutOfBoundsException");
                    } catch (IndexOutOfBoundsException | BufferOverflowException ignored) {
                    }

                    // position/limit may not have changed
                    assertThat(bb.position()).isEqualTo(0);
                    assertThat(bb.limit()).isEqualTo(bb.capacity());
                }
            }

            for (int off : invalidOffsets) {
                for (int len : validLengths) {
                    try {
                        seg.put(off, bb, len);
                        fail("should fail with an IndexOutOfBoundsException");
                    } catch (IndexOutOfBoundsException | BufferUnderflowException ignored) {
                    }

                    try {
                        seg.get(off, bb, len);
                        fail("should fail with an IndexOutOfBoundsException");
                    } catch (IndexOutOfBoundsException | BufferOverflowException ignored) {
                    }

                    // position/limit may not have changed
                    assertThat(bb.position()).isEqualTo(0);
                    assertThat(bb.limit()).isEqualTo(bb.capacity());
                }
            }

            for (int off : validOffsets) {
                for (int len : validLengths) {
                    if (off + len > pageSize) {
                        try {
                            seg.put(off, bb, len);
                            fail("should fail with an IndexOutOfBoundsException");
                        } catch (IndexOutOfBoundsException | BufferUnderflowException ignored) {
                        }

                        try {
                            seg.get(off, bb, len);
                            fail("should fail with an IndexOutOfBoundsException");
                        } catch (IndexOutOfBoundsException | BufferOverflowException ignored) {
                        }

                        // position/limit may not have changed
                        assertThat(bb.position()).isEqualTo(0);
                        assertThat(bb.limit()).isEqualTo(bb.capacity());
                    }
                }
            }
        }
    }

    @Test
    public void testByteBufferOverflowUnderflow() {
        final int bbCapacity = pageSize / 10;
        ByteBuffer bb = ByteBuffer.allocate(bbCapacity);

        MemorySegment seg = createSegment(pageSize);

        try {
            seg.get(pageSize / 5, bb, pageSize / 10 + 2);
            fail("should fail with an exception");
        } catch (BufferOverflowException ignored) {
        }

        // position / limit should not have been modified
        assertThat(bb.position()).isEqualTo(0);
        assertThat(bb.limit()).isEqualTo(bb.capacity());

        try {
            seg.put(pageSize / 5, bb, pageSize / 10 + 2);
            fail("should fail with an exception");
        } catch (BufferUnderflowException ignored) {
        }

        // position / limit should not have been modified
        assertThat(bb.position()).isEqualTo(0);
        assertThat(bb.limit()).isEqualTo(bb.capacity());

        int pos = bb.capacity() / 3;
        int limit = 2 * bb.capacity() / 3;
        bb.limit(limit);
        bb.position(pos);

        try {
            seg.get(20, bb, bb.capacity() / 3 + 3);
            fail("should fail with an exception");
        } catch (BufferOverflowException ignored) {
        }

        // position / limit should not have been modified
        assertThat(bb.position()).isEqualTo(pos);
        assertThat(bb.limit()).isEqualTo(limit);

        try {
            seg.put(20, bb, bb.capacity() / 3 + 3);
            fail("should fail with an exception");
        } catch (BufferUnderflowException ignored) {
        }

        // position / limit should not have been modified
        assertThat(bb.position()).isEqualTo(pos);
        assertThat(bb.limit()).isEqualTo(limit);
    }

    // ------------------------------------------------------------------------
    //  Comparing and swapping
    // ------------------------------------------------------------------------

    @Test
    public void testCompareBytes() {
        final byte[] bytes1 = new byte[pageSize];
        final byte[] bytes2 = new byte[pageSize];

        final int stride = pageSize / 255;
        final int shift = 16666;

        for (int i = 0; i < pageSize; i++) {
            byte val = (byte) ((i / stride) & 0xff);
            bytes1[i] = val;

            if (i + shift < bytes2.length) {
                bytes2[i + shift] = val;
            }
        }

        MemorySegment seg1 = createSegment(pageSize);
        MemorySegment seg2 = createSegment(pageSize);
        seg1.put(0, bytes1);
        seg2.put(0, bytes2);

        for (int i = 0; i < 1000; i++) {
            int pos1 = random.nextInt(bytes1.length);
            int pos2 = random.nextInt(bytes2.length);

            int len =
                    Math.min(
                            Math.min(bytes1.length - pos1, bytes2.length - pos2),
                            random.nextInt(pageSize / 50));

            int cmp = seg1.compare(seg2, pos1, pos2, len);

            if (pos1 < pos2 - shift) {
                assertThat(cmp <= 0).isTrue();
            } else {
                assertThat(cmp >= 0).isTrue();
            }
        }
    }

    @Test
    public void testCompareBytesWithDifferentLength() {
        final byte[] bytes1 = new byte[] {'a', 'b', 'c'};
        final byte[] bytes2 = new byte[] {'a', 'b', 'c', 'd'};

        MemorySegment seg1 = createSegment(4);
        MemorySegment seg2 = createSegment(4);
        seg1.put(0, bytes1);
        seg2.put(0, bytes2);

        assertThat(seg1.compare(seg2, 0, 0, 3, 4)).isLessThan(0);
        assertThat(seg1.compare(seg2, 0, 0, 3, 3)).isEqualTo(0);
        assertThat(seg1.compare(seg2, 0, 0, 3, 2)).isGreaterThan(0);
        // test non-zero offset
        assertThat(seg1.compare(seg2, 1, 1, 2, 3)).isLessThan(0);
        assertThat(seg1.compare(seg2, 1, 1, 2, 2)).isEqualTo(0);
        assertThat(seg1.compare(seg2, 1, 1, 2, 1)).isGreaterThan(0);
    }

    @Test
    public void testSwapBytes() {
        final int halfPageSize = pageSize / 2;

        final byte[] bytes1 = new byte[pageSize];
        final byte[] bytes2 = new byte[halfPageSize];

        Arrays.fill(bytes2, (byte) 1);

        MemorySegment seg1 = createSegment(pageSize);
        MemorySegment seg2 = createSegment(halfPageSize);
        seg1.put(0, bytes1);
        seg2.put(0, bytes2);

        // wap the second half of the first segment with the second segment

        int pos = 0;
        while (pos < halfPageSize) {
            int len = random.nextInt(pageSize / 40);
            len = Math.min(len, halfPageSize - pos);
            seg1.swapBytes(new byte[len], seg2, pos + halfPageSize, pos, len);
            pos += len;
        }

        // the second segment should now be all zeros, the first segment should have one in its
        // second half

        for (int i = 0; i < halfPageSize; i++) {
            assertThat(seg1.get(i)).isEqualTo((byte) 0);
            assertThat(seg2.get(i)).isEqualTo((byte) 0);
            assertThat(seg1.get(i + halfPageSize)).isEqualTo((byte) 1);
        }
    }

    @Test
    public void testCheckAgainstOverflowUnderflowOnRelease() {
        MemorySegment seg = createSegment(512);
        seg.free();

        // --- bytes (smallest type) ---
        try {
            seg.get(0);
            fail("Expecting an IllegalStateException");
        } catch (Exception e) {
            assertThat(e instanceof IllegalStateException || e instanceof NullPointerException)
                    .isTrue();
        }

        try {
            seg.get(Integer.MAX_VALUE);
            fail("Expecting an IllegalStateException");
        } catch (Exception e) {
            assertThat(e instanceof IllegalStateException || e instanceof NullPointerException)
                    .isTrue();
        }

        try {
            seg.get(Integer.MIN_VALUE);
            fail("Expecting an IllegalStateException");
        } catch (Exception e) {
            assertThat(e instanceof IllegalStateException || e instanceof NullPointerException)
                    .isTrue();
        }

        // --- longs (largest type) ---
        try {
            seg.getLong(0);
            fail("Expecting an IllegalStateException");
        } catch (Exception e) {
            assertThat(e instanceof IllegalStateException || e instanceof NullPointerException)
                    .isTrue();
        }

        try {
            seg.getLong(Integer.MAX_VALUE);
            fail("Expecting an IllegalStateException");
        } catch (Exception e) {
            assertThat(e instanceof IllegalStateException || e instanceof NullPointerException)
                    .isTrue();
        }

        try {
            seg.getLong(Integer.MIN_VALUE);
            fail("Expecting an IllegalStateException");
        } catch (Exception e) {
            assertThat(e instanceof IllegalStateException || e instanceof NullPointerException)
                    .isTrue();
        }
    }

    // ------------------------------------------------------------------------
    //  Miscellaneous
    // ------------------------------------------------------------------------

    @Test
    public void testByteBufferWrapping() {
        MemorySegment seg = createSegment(1024);

        ByteBuffer buf1 = seg.wrap(13, 47);
        assertThat(buf1.position()).isEqualTo(13);
        assertThat(buf1.limit()).isEqualTo(60);
        assertThat(buf1.remaining()).isEqualTo(47);

        ByteBuffer buf2 = seg.wrap(500, 267);
        assertThat(buf2.position()).isEqualTo(500);
        assertThat(buf2.limit()).isEqualTo(767);
        assertThat(buf2.remaining()).isEqualTo(267);

        ByteBuffer buf3 = seg.wrap(0, 1024);
        assertThat(buf3.position()).isEqualTo(0);
        assertThat(buf3.limit()).isEqualTo(1024);
        assertThat(buf3.remaining()).isEqualTo(1024);

        // verify that operations on the byte buffer are correctly reflected
        // in the memory segment
        buf3.order(ByteOrder.LITTLE_ENDIAN);
        buf3.putInt(112, 651797651);
        assertThat(seg.getIntLittleEndian(112)).isEqualTo(651797651);

        buf3.order(ByteOrder.BIG_ENDIAN);
        buf3.putInt(187, 992288337);
        assertThat(seg.getIntBigEndian(187)).isEqualTo(992288337);

        try {
            seg.wrap(-1, 20);
            fail("should throw an exception");
        } catch (IndexOutOfBoundsException | IllegalArgumentException ignored) {
        }

        try {
            seg.wrap(10, -20);
            fail("should throw an exception");
        } catch (IndexOutOfBoundsException | IllegalArgumentException ignored) {
        }

        try {
            seg.wrap(10, 1024);
            fail("should throw an exception");
        } catch (IndexOutOfBoundsException | IllegalArgumentException ignored) {
        }

        // after freeing, no wrapping should be possible any more.
        seg.free();

        try {
            seg.wrap(13, 47);
            fail("should fail with an exception");
        } catch (IllegalStateException e) {
            // expected
        }

        // existing wraps should stay valid after freeing
        buf3.order(ByteOrder.LITTLE_ENDIAN);
        buf3.putInt(112, 651797651);
        assertThat(buf3.getInt(112)).isEqualTo(651797651);
        buf3.order(ByteOrder.BIG_ENDIAN);
        buf3.putInt(187, 992288337);
        assertThat(buf3.getInt(187)).isEqualTo(992288337);
    }

    @Test
    public void testOwner() {
        // a segment without an owner has a null owner
        assertThat(createSegment(64).getOwner()).isNull();

        Object theOwner = new Object();
        MemorySegment seg = createSegment(64, theOwner);
        assertThat(seg.getOwner()).isEqualTo(theOwner);

        // freeing must release the owner, to prevent leaks that prevent class unloading!
        seg.free();
        assertThat(seg.getOwner()).isNotNull();
    }

    @Test
    public void testSizeAndFreeing() {
        // a segment without an owner has a null owner
        final int segmentSize = 651;
        MemorySegment seg = createSegment(segmentSize);

        assertThat(seg.size()).isEqualTo(segmentSize);
        assertThat(seg.isFreed()).isFalse();

        seg.free();
        assertThat(seg.isFreed()).isTrue();
        assertThat(seg.size()).isEqualTo(segmentSize);
    }

    // ------------------------------------------------------------------------
    //  Parametrization to run with different segment sizes
    // ------------------------------------------------------------------------

    @Parameterized.Parameters(name = "segment-size = {0}")
    public static Collection<Object[]> executionModes() {
        return Arrays.asList(
                new Object[] {32 * 1024}, new Object[] {4 * 1024}, new Object[] {512 * 1024});
    }
}
