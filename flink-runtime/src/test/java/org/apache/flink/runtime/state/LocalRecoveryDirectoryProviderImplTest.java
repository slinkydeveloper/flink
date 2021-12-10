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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link LocalRecoveryDirectoryProvider}. */
public class LocalRecoveryDirectoryProviderImplTest extends TestLogger {

    private static final JobID JOB_ID = new JobID();
    private static final JobVertexID JOB_VERTEX_ID = new JobVertexID();
    private static final int SUBTASK_INDEX = 0;

    @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

    private LocalRecoveryDirectoryProviderImpl directoryProvider;
    private File[] allocBaseFolders;

    @Before
    public void setup() throws IOException {
        this.allocBaseFolders =
                new File[] {tmpFolder.newFolder(), tmpFolder.newFolder(), tmpFolder.newFolder()};
        this.directoryProvider =
                new LocalRecoveryDirectoryProviderImpl(
                        allocBaseFolders, JOB_ID, JOB_VERTEX_ID, SUBTASK_INDEX);
    }

    @Test
    public void allocationBaseDir() {
        for (int i = 0; i < 10; ++i) {
            assertThat(directoryProvider.allocationBaseDirectory(i))
                    .isEqualTo(allocBaseFolders[i % allocBaseFolders.length]);
        }
    }

    @Test
    public void selectAllocationBaseDir() {
        for (int i = 0; i < allocBaseFolders.length; ++i) {
            assertThat(directoryProvider.selectAllocationBaseDirectory(i))
                    .isEqualTo(allocBaseFolders[i]);
        }
    }

    @Test
    public void allocationBaseDirectoriesCount() {
        assertThat(directoryProvider.allocationBaseDirsCount()).isEqualTo(allocBaseFolders.length);
    }

    @Test
    public void subtaskSpecificDirectory() {
        for (int i = 0; i < 10; ++i) {
            assertThat(directoryProvider.subtaskBaseDirectory(i))
                    .isEqualTo(
                            new File(
                                    directoryProvider.allocationBaseDirectory(i),
                                    directoryProvider.subtaskDirString()));
        }
    }

    @Test
    public void subtaskCheckpointSpecificDirectory() {
        for (int i = 0; i < 10; ++i) {
            assertThat(directoryProvider.subtaskSpecificCheckpointDirectory(i))
                    .isEqualTo(
                            new File(
                                    directoryProvider.subtaskBaseDirectory(i),
                                    directoryProvider.checkpointDirString(i)));
        }
    }

    @Test
    public void testPathStringConstants() {

        assertThat(
                        "jid_"
                                + JOB_ID
                                + Path.SEPARATOR
                                + "vtx_"
                                + JOB_VERTEX_ID
                                + "_sti_"
                                + SUBTASK_INDEX)
                .isEqualTo(directoryProvider.subtaskDirString());

        final long checkpointId = 42;
        assertThat("chk_" + checkpointId)
                .isEqualTo(directoryProvider.checkpointDirString(checkpointId));
    }

    @Test
    public void testPreconditionsNotNullFiles() {
        try {
            new LocalRecoveryDirectoryProviderImpl(
                    new File[] {null}, JOB_ID, JOB_VERTEX_ID, SUBTASK_INDEX);
            fail("unknown failure");
        } catch (NullPointerException ignore) {
        }
    }
}
