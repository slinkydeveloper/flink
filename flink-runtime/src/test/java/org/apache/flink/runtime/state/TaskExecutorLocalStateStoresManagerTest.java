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
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.VoidPermanentBlobService;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.taskexecutor.TaskExecutorResourceUtils;
import org.apache.flink.runtime.taskexecutor.TaskManagerServices;
import org.apache.flink.runtime.taskexecutor.TaskManagerServicesConfiguration;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.Executors;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.InetAddress;

import static org.assertj.core.api.Assertions.assertThat;

public class TaskExecutorLocalStateStoresManagerTest extends TestLogger {

    @ClassRule public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final int TOTAL_FLINK_MEMORY_MB = 1024;

    /**
     * This tests that the creation of {@link TaskManagerServices} correctly creates the local state
     * root directory for the {@link TaskExecutorLocalStateStoresManager} with the configured root
     * directory.
     */
    @Test
    public void testCreationFromConfig() throws Exception {

        final Configuration config = new Configuration();

        File newFolder = temporaryFolder.newFolder();
        String tmpDir = newFolder.getAbsolutePath() + File.separator;
        final String rootDirString =
                "__localStateRoot1,__localStateRoot2,__localStateRoot3".replaceAll("__", tmpDir);

        // test configuration of the local state directories
        config.setString(
                CheckpointingOptions.LOCAL_RECOVERY_TASK_MANAGER_STATE_ROOT_DIRS, rootDirString);

        // test configuration of the local state mode
        config.setBoolean(CheckpointingOptions.LOCAL_RECOVERY, true);

        TaskManagerServices taskManagerServices =
                createTaskManagerServices(createTaskManagerServiceConfiguration(config));

        try {
            TaskExecutorLocalStateStoresManager taskStateManager =
                    taskManagerServices.getTaskManagerStateStore();

            // verify configured directories for local state
            String[] split = rootDirString.split(",");
            File[] rootDirectories = taskStateManager.getLocalStateRootDirectories();
            for (int i = 0; i < split.length; ++i) {
                assertThat(rootDirectories[i])
                        .isEqualTo(
                                new File(
                                        split[i],
                                        TaskManagerServices.LOCAL_STATE_SUB_DIRECTORY_ROOT));
            }

            // verify local recovery mode
            assertThat(taskStateManager.isLocalRecoveryEnabled()).isTrue();

            assertThat(TaskManagerServices.LOCAL_STATE_SUB_DIRECTORY_ROOT).isEqualTo("localState");
            for (File rootDirectory : rootDirectories) {
                FileUtils.deleteFileOrDirectory(rootDirectory);
            }
        } finally {
            taskManagerServices.shutDown();
        }
    }

    /**
     * This tests that the creation of {@link TaskManagerServices} correctly falls back to the first
     * tmp directory of the IOManager as default for the local state root directory.
     */
    @Test
    public void testCreationFromConfigDefault() throws Exception {

        final Configuration config = new Configuration();

        TaskManagerServicesConfiguration taskManagerServicesConfiguration =
                createTaskManagerServiceConfiguration(config);

        TaskManagerServices taskManagerServices =
                createTaskManagerServices(taskManagerServicesConfiguration);

        try {
            TaskExecutorLocalStateStoresManager taskStateManager =
                    taskManagerServices.getTaskManagerStateStore();

            String[] tmpDirPaths = taskManagerServicesConfiguration.getTmpDirPaths();
            File[] localStateRootDirectories = taskStateManager.getLocalStateRootDirectories();

            for (int i = 0; i < tmpDirPaths.length; ++i) {
                assertThat(localStateRootDirectories[i])
                        .isEqualTo(
                                new File(
                                        tmpDirPaths[i],
                                        TaskManagerServices.LOCAL_STATE_SUB_DIRECTORY_ROOT));
            }

            assertThat(taskStateManager.isLocalRecoveryEnabled()).isFalse();
        } finally {
            taskManagerServices.shutDown();
        }
    }

    /**
     * This tests that the {@link TaskExecutorLocalStateStoresManager} creates {@link
     * TaskLocalStateStoreImpl} that have a properly initialized local state base directory. It also
     * checks that subdirectories are correctly deleted on shutdown.
     */
    @Test
    public void testSubtaskStateStoreDirectoryCreateAndDelete() throws Exception {

        JobID jobID = new JobID();
        JobVertexID jobVertexID = new JobVertexID();
        AllocationID allocationID = new AllocationID();
        int subtaskIdx = 23;

        File[] rootDirs = {
            temporaryFolder.newFolder(), temporaryFolder.newFolder(), temporaryFolder.newFolder()
        };
        TaskExecutorLocalStateStoresManager storesManager =
                new TaskExecutorLocalStateStoresManager(true, rootDirs, Executors.directExecutor());

        TaskLocalStateStore taskLocalStateStore =
                storesManager.localStateStoreForSubtask(
                        jobID, allocationID, jobVertexID, subtaskIdx);

        LocalRecoveryDirectoryProvider directoryProvider =
                taskLocalStateStore.getLocalRecoveryConfig().getLocalStateDirectoryProvider();

        for (int i = 0; i < 10; ++i) {
            assertThat(directoryProvider.allocationBaseDirectory(i))
                    .isEqualTo(
                            new File(
                                    rootDirs[(i & Integer.MAX_VALUE) % rootDirs.length],
                                    storesManager.allocationSubDirString(allocationID)));
        }

        long chkId = 42L;
        File allocBaseDirChk42 = directoryProvider.allocationBaseDirectory(chkId);
        File subtaskSpecificCheckpointDirectory =
                directoryProvider.subtaskSpecificCheckpointDirectory(chkId);
        assertThat(subtaskSpecificCheckpointDirectory)
                .isEqualTo(
                        new File(
                                allocBaseDirChk42,
                                "jid_"
                                        + jobID
                                        + File.separator
                                        + "vtx_"
                                        + jobVertexID
                                        + "_"
                                        + "sti_"
                                        + subtaskIdx
                                        + File.separator
                                        + "chk_"
                                        + chkId));

        assertThat(subtaskSpecificCheckpointDirectory.mkdirs()).isTrue();

        File testFile = new File(subtaskSpecificCheckpointDirectory, "test");
        assertThat(testFile.createNewFile()).isTrue();

        // test that local recovery mode is forwarded to the created store
        assertThat(taskLocalStateStore.getLocalRecoveryConfig().isLocalRecoveryEnabled())
                .isEqualTo(storesManager.isLocalRecoveryEnabled());

        assertThat(testFile.exists()).isTrue();

        // check cleanup after releasing allocation id
        storesManager.releaseLocalStateForAllocationId(allocationID);
        checkRootDirsClean(rootDirs);

        AllocationID otherAllocationID = new AllocationID();

        taskLocalStateStore =
                storesManager.localStateStoreForSubtask(
                        jobID, otherAllocationID, jobVertexID, subtaskIdx);

        directoryProvider =
                taskLocalStateStore.getLocalRecoveryConfig().getLocalStateDirectoryProvider();

        File chkDir = directoryProvider.subtaskSpecificCheckpointDirectory(23L);
        assertThat(chkDir.mkdirs()).isTrue();
        testFile = new File(chkDir, "test");
        assertThat(testFile.createNewFile()).isTrue();

        // check cleanup after shutdown
        storesManager.shutdown();
        checkRootDirsClean(rootDirs);
    }

    private void checkRootDirsClean(File[] rootDirs) {
        for (File rootDir : rootDirs) {
            File[] files = rootDir.listFiles();
            if (files != null) {
                assertThat(files).isEqualTo(new File[0]);
            }
        }
    }

    private TaskManagerServicesConfiguration createTaskManagerServiceConfiguration(
            Configuration config) throws Exception {
        return TaskManagerServicesConfiguration.fromConfiguration(
                config,
                ResourceID.generate(),
                InetAddress.getLocalHost().getHostName(),
                true,
                TaskExecutorResourceUtils.resourceSpecFromConfigForLocalExecution(config));
    }

    private TaskManagerServices createTaskManagerServices(TaskManagerServicesConfiguration config)
            throws Exception {
        return TaskManagerServices.fromConfiguration(
                config,
                VoidPermanentBlobService.INSTANCE,
                UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
                Executors.newDirectExecutorService(),
                throwable -> {});
    }
}
