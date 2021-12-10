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

import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createFilledFinishedBufferConsumer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Basic subpartition behaviour tests. */
public abstract class SubpartitionTestBase extends TestLogger {

    /** Return the subpartition to be tested. */
    abstract ResultSubpartition createSubpartition() throws Exception;

    /** Return the subpartition to be used for tests where write calls should fail. */
    abstract ResultSubpartition createFailingWritesSubpartition() throws Exception;

    // ------------------------------------------------------------------------

    @Test
    public void createReaderAfterDispose() throws Exception {
        final ResultSubpartition subpartition = createSubpartition();
        subpartition.release();

        try {
            subpartition.createReadView(() -> {});
            fail("expected an exception");
        } catch (IllegalStateException e) {
            // expected
        }
    }

    @Test
    public void testAddAfterFinish() throws Exception {
        final ResultSubpartition subpartition = createSubpartition();

        try {
            subpartition.finish();
            assertThat(subpartition.getTotalNumberOfBuffers()).isEqualTo(1);
            assertThat(subpartition.getBuffersInBacklogUnsafe()).isEqualTo(0);

            BufferConsumer bufferConsumer = createFilledFinishedBufferConsumer(4096);

            assertThat(subpartition.add(bufferConsumer)).isEqualTo(-1);
            assertThat(bufferConsumer.isRecycled()).isTrue();

            assertThat(subpartition.getTotalNumberOfBuffers()).isEqualTo(1);
            assertThat(subpartition.getBuffersInBacklogUnsafe()).isEqualTo(0);
        } finally {
            if (subpartition != null) {
                subpartition.release();
            }
        }
    }

    @Test
    public void testAddAfterRelease() throws Exception {
        final ResultSubpartition subpartition = createSubpartition();

        try {
            subpartition.release();

            BufferConsumer bufferConsumer = createFilledFinishedBufferConsumer(4096);

            assertThat(subpartition.add(bufferConsumer)).isEqualTo(-1);
            assertThat(bufferConsumer.isRecycled()).isTrue();

        } finally {
            if (subpartition != null) {
                subpartition.release();
            }
        }
    }

    @Test
    public void testReleasingReaderDoesNotReleasePartition() throws Exception {
        final ResultSubpartition partition = createSubpartition();
        partition.add(createFilledFinishedBufferConsumer(BufferBuilderTestUtils.BUFFER_SIZE));
        partition.finish();

        final ResultSubpartitionView reader =
                partition.createReadView(new NoOpBufferAvailablityListener());

        assertThat(partition.isReleased()).isFalse();
        assertThat(reader.isReleased()).isFalse();

        reader.releaseAllResources();

        assertThat(reader.isReleased()).isTrue();
        assertThat(partition.isReleased()).isFalse();

        partition.release();
    }

    @Test
    public void testReleaseIsIdempotent() throws Exception {
        final ResultSubpartition partition = createSubpartition();
        partition.add(createFilledFinishedBufferConsumer(BufferBuilderTestUtils.BUFFER_SIZE));
        partition.finish();

        partition.release();
        partition.release();
        partition.release();
    }

    @Test
    public void testReadAfterDispose() throws Exception {
        final ResultSubpartition partition = createSubpartition();
        partition.add(createFilledFinishedBufferConsumer(BufferBuilderTestUtils.BUFFER_SIZE));
        partition.finish();

        final ResultSubpartitionView reader =
                partition.createReadView(new NoOpBufferAvailablityListener());
        reader.releaseAllResources();

        // the reader must not throw an exception
        reader.getNextBuffer();

        // ideally, we want this to be null, but the pipelined partition still serves data
        // after dispose (which is unintuitive, but does not affect correctness)
        //		assertNull(reader.getNextBuffer());
    }

    @Test
    public void testRecycleBufferAndConsumerOnFailure() throws Exception {
        final ResultSubpartition subpartition = createFailingWritesSubpartition();
        try {
            final BufferConsumer consumer =
                    BufferBuilderTestUtils.createFilledFinishedBufferConsumer(100);

            try {
                subpartition.add(consumer);
                subpartition.flush();
                fail("should fail with an exception");
            } catch (Exception ignored) {
                // expected
            }

            assertThat(consumer.isRecycled()).isTrue();
        } finally {
            subpartition.release();
        }
    }
}
