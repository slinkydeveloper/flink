/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.util.IOUtils;

import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.util.SizeUnit;

import java.io.File;
import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.contrib.streaming.state.RocksDBTestUtils.createKeyedStateBackend;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.HamcrestCondition.matching;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for configuring the RocksDB State Backend. */
@SuppressWarnings("serial")
public class RocksDBStateBackendConfigTest {

    @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

    // ------------------------------------------------------------------------
    //  default values
    // ------------------------------------------------------------------------

    @Test
    public void testDefaultsInSync() throws Exception {
        final boolean defaultIncremental =
                CheckpointingOptions.INCREMENTAL_CHECKPOINTS.defaultValue();

        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        assertThat(backend.isIncrementalCheckpointsEnabled()).isEqualTo(defaultIncremental);
    }

    // ------------------------------------------------------------------------
    //  RocksDB local file directory
    // ------------------------------------------------------------------------

    /** This test checks the behavior for basic setting of local DB directories. */
    @Test
    public void testSetDbPath() throws Exception {
        final EmbeddedRocksDBStateBackend rocksDbBackend = new EmbeddedRocksDBStateBackend();

        final String testDir1 = tempFolder.newFolder().getAbsolutePath();
        final String testDir2 = tempFolder.newFolder().getAbsolutePath();

        assertThat(rocksDbBackend.getDbStoragePaths()).isNull();

        rocksDbBackend.setDbStoragePath(testDir1);
        assertThat(rocksDbBackend.getDbStoragePaths()).isEqualTo(new String[] {testDir1});

        rocksDbBackend.setDbStoragePath(null);
        assertThat(rocksDbBackend.getDbStoragePaths()).isNull();

        rocksDbBackend.setDbStoragePaths(testDir1, testDir2);
        assertThat(rocksDbBackend.getDbStoragePaths()).isEqualTo(new String[] {testDir1, testDir2});

        final MockEnvironment env = getMockEnvironment(tempFolder.newFolder());
        final RocksDBKeyedStateBackend<Integer> keyedBackend =
                createKeyedStateBackend(rocksDbBackend, env, IntSerializer.INSTANCE);

        try {
            File instanceBasePath = keyedBackend.getInstanceBasePath();
            assertThat(instanceBasePath.getAbsolutePath())
                    .satisfies(matching(anyOf(startsWith(testDir1), startsWith(testDir2))));

            //noinspection NullArgumentToVariableArgMethod
            rocksDbBackend.setDbStoragePaths(null);
            assertThat(rocksDbBackend.getDbStoragePaths()).isNull();
        } finally {
            IOUtils.closeQuietly(keyedBackend);
            keyedBackend.dispose();
            env.close();
        }
    }

    @Test
    public void testConfigureTimerService() throws Exception {

        final MockEnvironment env = getMockEnvironment(tempFolder.newFolder());

        // Fix the option key string
        assertThat(RocksDBOptions.TIMER_SERVICE_FACTORY.key())
                .isEqualTo("state.backend.rocksdb.timer-service.factory");

        // Fix the option value string and ensure all are covered
        assertThat(EmbeddedRocksDBStateBackend.PriorityQueueStateType.values().length).isEqualTo(2);
        assertThat(EmbeddedRocksDBStateBackend.PriorityQueueStateType.ROCKSDB.toString())
                .isEqualTo("ROCKSDB");
        assertThat(EmbeddedRocksDBStateBackend.PriorityQueueStateType.HEAP.toString())
                .isEqualTo("HEAP");

        // Fix the default
        assertThat(RocksDBOptions.TIMER_SERVICE_FACTORY.defaultValue())
                .isEqualTo(EmbeddedRocksDBStateBackend.PriorityQueueStateType.ROCKSDB);

        EmbeddedRocksDBStateBackend rocksDbBackend = new EmbeddedRocksDBStateBackend();

        RocksDBKeyedStateBackend<Integer> keyedBackend =
                createKeyedStateBackend(rocksDbBackend, env, IntSerializer.INSTANCE);
        assertThat(keyedBackend.getPriorityQueueFactory().getClass())
                .isEqualTo(RocksDBPriorityQueueSetFactory.class);
        keyedBackend.dispose();

        Configuration conf = new Configuration();
        conf.set(
                RocksDBOptions.TIMER_SERVICE_FACTORY,
                EmbeddedRocksDBStateBackend.PriorityQueueStateType.HEAP);

        rocksDbBackend =
                rocksDbBackend.configure(conf, Thread.currentThread().getContextClassLoader());
        keyedBackend = createKeyedStateBackend(rocksDbBackend, env, IntSerializer.INSTANCE);
        assertThat(keyedBackend.getPriorityQueueFactory().getClass())
                .isEqualTo(HeapPriorityQueueSetFactory.class);
        keyedBackend.dispose();
        env.close();
    }

    /** Validates that user custom configuration from code should override the flink-conf.yaml. */
    @Test
    public void testConfigureTimerServiceLoadingFromApplication() throws Exception {
        final MockEnvironment env = new MockEnvironmentBuilder().build();

        // priorityQueueStateType of the job backend
        final EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();
        backend.setPriorityQueueStateType(EmbeddedRocksDBStateBackend.PriorityQueueStateType.HEAP);

        // priorityQueueStateType in the cluster config
        final Configuration configFromConfFile = new Configuration();
        configFromConfFile.setString(
                RocksDBOptions.TIMER_SERVICE_FACTORY.key(),
                RocksDBStateBackend.PriorityQueueStateType.ROCKSDB.toString());

        // configure final backend from job and cluster config
        final EmbeddedRocksDBStateBackend configuredRocksDBStateBackend =
                backend.configure(
                        configFromConfFile, Thread.currentThread().getContextClassLoader());
        final RocksDBKeyedStateBackend<Integer> keyedBackend =
                createKeyedStateBackend(configuredRocksDBStateBackend, env, IntSerializer.INSTANCE);

        // priorityQueueStateType of the job backend should be preserved
        assertThat(keyedBackend.getPriorityQueueFactory())
                .isInstanceOf(HeapPriorityQueueSetFactory.class);

        keyedBackend.close();
        keyedBackend.dispose();
        env.close();
    }

    @Test
    public void testStoragePathWithFilePrefix() throws Exception {
        final File folder = tempFolder.newFolder();
        final String dbStoragePath = new Path(folder.toURI().toString()).toString();

        assertThat(dbStoragePath.startsWith("file:")).isTrue();

        testLocalDbPaths(dbStoragePath, folder);
    }

    @Test
    public void testWithDefaultFsSchemeNoStoragePath() throws Exception {
        try {
            // set the default file system scheme
            Configuration config = new Configuration();
            config.setString(CoreOptions.DEFAULT_FILESYSTEM_SCHEME, "s3://mydomain.com:8020/flink");
            FileSystem.initialize(config);
            testLocalDbPaths(null, tempFolder.getRoot());
        } finally {
            FileSystem.initialize(new Configuration());
        }
    }

    @Test
    public void testWithDefaultFsSchemeAbsoluteStoragePath() throws Exception {
        final File folder = tempFolder.newFolder();
        final String dbStoragePath = folder.getAbsolutePath();

        try {
            // set the default file system scheme
            Configuration config = new Configuration();
            config.setString(CoreOptions.DEFAULT_FILESYSTEM_SCHEME, "s3://mydomain.com:8020/flink");
            FileSystem.initialize(config);

            testLocalDbPaths(dbStoragePath, folder);
        } finally {
            FileSystem.initialize(new Configuration());
        }
    }

    private void testLocalDbPaths(String configuredPath, File expectedPath) throws Exception {
        final EmbeddedRocksDBStateBackend rocksDbBackend = new EmbeddedRocksDBStateBackend();
        rocksDbBackend.setDbStoragePath(configuredPath);

        final MockEnvironment env = getMockEnvironment(tempFolder.newFolder());
        RocksDBKeyedStateBackend<Integer> keyedBackend =
                createKeyedStateBackend(rocksDbBackend, env, IntSerializer.INSTANCE);

        try {
            File instanceBasePath = keyedBackend.getInstanceBasePath();
            assertThat(instanceBasePath.getAbsolutePath())
                    .startsWith(expectedPath.getAbsolutePath());

            //noinspection NullArgumentToVariableArgMethod
            rocksDbBackend.setDbStoragePaths(null);
            assertThat(rocksDbBackend.getDbStoragePaths()).isNull();
        } finally {
            IOUtils.closeQuietly(keyedBackend);
            keyedBackend.dispose();
            env.close();
        }
    }

    /** Validates that empty arguments for the local DB path are invalid. */
    @Test(expected = IllegalArgumentException.class)
    public void testSetEmptyPaths() throws Exception {
        String checkpointPath = tempFolder.newFolder().toURI().toString();
        RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(checkpointPath);
        rocksDbBackend.setDbStoragePaths();
    }

    /** Validates that schemes other than 'file:/' are not allowed. */
    @Test(expected = IllegalArgumentException.class)
    public void testNonFileSchemePath() throws Exception {
        String checkpointPath = tempFolder.newFolder().toURI().toString();
        RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(checkpointPath);
        rocksDbBackend.setDbStoragePath("hdfs:///some/path/to/perdition");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDbPathRelativePaths() throws Exception {
        RocksDBStateBackend rocksDbBackend =
                new RocksDBStateBackend(tempFolder.newFolder().toURI().toString());
        rocksDbBackend.setDbStoragePath("relative/path");
    }

    // ------------------------------------------------------------------------
    //  RocksDB local file automatic from temp directories
    // ------------------------------------------------------------------------

    /**
     * This tests whether the RocksDB backends uses the temp directories that are provided from the
     * {@link Environment} when no db storage path is set.
     */
    @Test
    public void testUseTempDirectories() throws Exception {
        String checkpointPath = tempFolder.newFolder().toURI().toString();
        RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(checkpointPath);

        File dir1 = tempFolder.newFolder();
        File dir2 = tempFolder.newFolder();

        assertThat(rocksDbBackend.getDbStoragePaths()).isNull();

        final MockEnvironment env = getMockEnvironment(dir1, dir2);
        RocksDBKeyedStateBackend<Integer> keyedBackend =
                (RocksDBKeyedStateBackend<Integer>)
                        rocksDbBackend.createKeyedStateBackend(
                                env,
                                env.getJobID(),
                                "test_op",
                                IntSerializer.INSTANCE,
                                1,
                                new KeyGroupRange(0, 0),
                                env.getTaskKvStateRegistry(),
                                TtlTimeProvider.DEFAULT,
                                new UnregisteredMetricsGroup(),
                                Collections.emptyList(),
                                new CloseableRegistry());

        try {
            File instanceBasePath = keyedBackend.getInstanceBasePath();
            assertThat(instanceBasePath.getAbsolutePath())
                    .satisfies(
                            matching(
                                    anyOf(
                                            startsWith(dir1.getAbsolutePath()),
                                            startsWith(dir2.getAbsolutePath()))));
        } finally {
            IOUtils.closeQuietly(keyedBackend);
            keyedBackend.dispose();
            env.close();
        }
    }

    // ------------------------------------------------------------------------
    //  RocksDB local file directory initialization
    // ------------------------------------------------------------------------

    @Test
    public void testFailWhenNoLocalStorageDir() throws Exception {
        final File targetDir = tempFolder.newFolder();
        Assume.assumeTrue(
                "Cannot mark directory non-writable", targetDir.setWritable(false, false));

        String checkpointPath = tempFolder.newFolder().toURI().toString();
        RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(checkpointPath);

        try (MockEnvironment env = getMockEnvironment(tempFolder.newFolder())) {
            rocksDbBackend.setDbStoragePath(targetDir.getAbsolutePath());

            boolean hasFailure = false;
            try {
                rocksDbBackend.createKeyedStateBackend(
                        env,
                        env.getJobID(),
                        "foobar",
                        IntSerializer.INSTANCE,
                        1,
                        new KeyGroupRange(0, 0),
                        new KvStateRegistry().createTaskRegistry(env.getJobID(), new JobVertexID()),
                        TtlTimeProvider.DEFAULT,
                        new UnregisteredMetricsGroup(),
                        Collections.emptyList(),
                        new CloseableRegistry());
            } catch (Exception e) {
                assertThat(e.getMessage().contains("No local storage directories available"))
                        .isTrue();
                assertThat(e.getMessage().contains(targetDir.getAbsolutePath())).isTrue();
                hasFailure = true;
            }
            assertThat(hasFailure)
                    .as("We must see a failure because no storaged directory is feasible.")
                    .isTrue();
        } finally {
            //noinspection ResultOfMethodCallIgnored
            targetDir.setWritable(true, false);
        }
    }

    @Test
    public void testContinueOnSomeDbDirectoriesMissing() throws Exception {
        final File targetDir1 = tempFolder.newFolder();
        final File targetDir2 = tempFolder.newFolder();
        Assume.assumeTrue(
                "Cannot mark directory non-writable", targetDir1.setWritable(false, false));

        String checkpointPath = tempFolder.newFolder().toURI().toString();
        RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(checkpointPath);

        try (MockEnvironment env = getMockEnvironment(tempFolder.newFolder())) {
            rocksDbBackend.setDbStoragePaths(
                    targetDir1.getAbsolutePath(), targetDir2.getAbsolutePath());

            try {
                AbstractKeyedStateBackend<Integer> keyedStateBackend =
                        rocksDbBackend.createKeyedStateBackend(
                                env,
                                env.getJobID(),
                                "foobar",
                                IntSerializer.INSTANCE,
                                1,
                                new KeyGroupRange(0, 0),
                                new KvStateRegistry()
                                        .createTaskRegistry(env.getJobID(), new JobVertexID()),
                                TtlTimeProvider.DEFAULT,
                                new UnregisteredMetricsGroup(),
                                Collections.emptyList(),
                                new CloseableRegistry());

                IOUtils.closeQuietly(keyedStateBackend);
                keyedStateBackend.dispose();
            } catch (Exception e) {
                e.printStackTrace();
                fail("Backend initialization failed even though some paths were available");
            }
        } finally {
            //noinspection ResultOfMethodCallIgnored
            targetDir1.setWritable(true, false);
        }
    }

    // ------------------------------------------------------------------------
    //  RocksDB Options
    // ------------------------------------------------------------------------

    @Test
    public void testPredefinedOptions() throws Exception {
        String checkpointPath = tempFolder.newFolder().toURI().toString();
        RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(checkpointPath);

        // verify that we would use PredefinedOptions.DEFAULT by default.
        assertThat(rocksDbBackend.getPredefinedOptions()).isEqualTo(PredefinedOptions.DEFAULT);

        // verify that user could configure predefined options via flink-conf.yaml
        Configuration configuration = new Configuration();
        configuration.setString(
                RocksDBOptions.PREDEFINED_OPTIONS, PredefinedOptions.FLASH_SSD_OPTIMIZED.name());
        rocksDbBackend = new RocksDBStateBackend(checkpointPath);
        rocksDbBackend = rocksDbBackend.configure(configuration, getClass().getClassLoader());
        assertThat(rocksDbBackend.getPredefinedOptions())
                .isEqualTo(PredefinedOptions.FLASH_SSD_OPTIMIZED);

        // verify that predefined options could be set programmatically and override pre-configured
        // one.
        rocksDbBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED);
        assertThat(rocksDbBackend.getPredefinedOptions())
                .isEqualTo(PredefinedOptions.SPINNING_DISK_OPTIMIZED);
    }

    @Test
    public void testConfigurableOptionsFromConfig() throws Exception {
        Configuration configuration = new Configuration();

        // verify illegal configuration
        {
            verifyIllegalArgument(RocksDBConfigurableOptions.MAX_BACKGROUND_THREADS, "-1");
            verifyIllegalArgument(RocksDBConfigurableOptions.LOG_LEVEL, "DEBUG");
            verifyIllegalArgument(RocksDBConfigurableOptions.LOG_DIR, "tmp/rocksdb-logs/");
            verifyIllegalArgument(RocksDBConfigurableOptions.LOG_DIR, "");
            verifyIllegalArgument(RocksDBConfigurableOptions.LOG_FILE_NUM, "0");
            verifyIllegalArgument(RocksDBConfigurableOptions.LOG_FILE_NUM, "-1");
            verifyIllegalArgument(RocksDBConfigurableOptions.LOG_MAX_FILE_SIZE, "-1KB");
            verifyIllegalArgument(RocksDBConfigurableOptions.MAX_WRITE_BUFFER_NUMBER, "-1");
            verifyIllegalArgument(
                    RocksDBConfigurableOptions.MIN_WRITE_BUFFER_NUMBER_TO_MERGE, "-1");

            verifyIllegalArgument(RocksDBConfigurableOptions.TARGET_FILE_SIZE_BASE, "0KB");
            verifyIllegalArgument(RocksDBConfigurableOptions.MAX_SIZE_LEVEL_BASE, "1BB");
            verifyIllegalArgument(RocksDBConfigurableOptions.WRITE_BUFFER_SIZE, "-1KB");
            verifyIllegalArgument(RocksDBConfigurableOptions.BLOCK_SIZE, "0MB");
            verifyIllegalArgument(RocksDBConfigurableOptions.METADATA_BLOCK_SIZE, "0MB");
            verifyIllegalArgument(RocksDBConfigurableOptions.BLOCK_CACHE_SIZE, "0");

            verifyIllegalArgument(RocksDBConfigurableOptions.USE_DYNAMIC_LEVEL_SIZE, "1");

            verifyIllegalArgument(RocksDBConfigurableOptions.COMPACTION_STYLE, "LEV");
            verifyIllegalArgument(RocksDBConfigurableOptions.USE_BLOOM_FILTER, "NO");
            verifyIllegalArgument(RocksDBConfigurableOptions.BLOOM_FILTER_BLOCK_BASED_MODE, "YES");
        }

        // verify legal configuration
        {
            configuration.setString(RocksDBConfigurableOptions.LOG_LEVEL.key(), "DEBUG_LEVEL");
            configuration.setString(RocksDBConfigurableOptions.LOG_DIR.key(), "/tmp/rocksdb-logs/");
            configuration.setString(RocksDBConfigurableOptions.LOG_FILE_NUM.key(), "10");
            configuration.setString(RocksDBConfigurableOptions.LOG_MAX_FILE_SIZE.key(), "2MB");
            configuration.setString(RocksDBConfigurableOptions.COMPACTION_STYLE.key(), "level");
            configuration.setString(
                    RocksDBConfigurableOptions.USE_DYNAMIC_LEVEL_SIZE.key(), "TRUE");
            configuration.setString(RocksDBConfigurableOptions.TARGET_FILE_SIZE_BASE.key(), "8 mb");
            configuration.setString(RocksDBConfigurableOptions.MAX_SIZE_LEVEL_BASE.key(), "128MB");
            configuration.setString(RocksDBConfigurableOptions.MAX_BACKGROUND_THREADS.key(), "4");
            configuration.setString(RocksDBConfigurableOptions.MAX_WRITE_BUFFER_NUMBER.key(), "4");
            configuration.setString(
                    RocksDBConfigurableOptions.MIN_WRITE_BUFFER_NUMBER_TO_MERGE.key(), "2");
            configuration.setString(RocksDBConfigurableOptions.WRITE_BUFFER_SIZE.key(), "64 MB");
            configuration.setString(RocksDBConfigurableOptions.BLOCK_SIZE.key(), "4 kb");
            configuration.setString(RocksDBConfigurableOptions.METADATA_BLOCK_SIZE.key(), "8 kb");
            configuration.setString(RocksDBConfigurableOptions.BLOCK_CACHE_SIZE.key(), "512 mb");
            configuration.setString(RocksDBConfigurableOptions.USE_BLOOM_FILTER.key(), "TRUE");

            try (RocksDBResourceContainer optionsContainer =
                    new RocksDBResourceContainer(
                            configuration, PredefinedOptions.DEFAULT, null, null)) {

                DBOptions dbOptions = optionsContainer.getDbOptions();
                assertThat(dbOptions.maxOpenFiles()).isEqualTo(-1);
                assertThat(dbOptions.infoLogLevel()).isEqualTo(InfoLogLevel.DEBUG_LEVEL);
                assertThat(dbOptions.dbLogDir()).isEqualTo("/tmp/rocksdb-logs/");
                assertThat(dbOptions.keepLogFileNum()).isEqualTo(10);
                assertThat(dbOptions.maxLogFileSize()).isEqualTo(2 * SizeUnit.MB);

                ColumnFamilyOptions columnOptions = optionsContainer.getColumnOptions();
                assertThat(columnOptions.compactionStyle()).isEqualTo(CompactionStyle.LEVEL);
                assertThat(columnOptions.levelCompactionDynamicLevelBytes()).isTrue();
                assertThat(columnOptions.targetFileSizeBase()).isEqualTo(8 * SizeUnit.MB);
                assertThat(columnOptions.maxBytesForLevelBase()).isEqualTo(128 * SizeUnit.MB);
                assertThat(columnOptions.maxWriteBufferNumber()).isEqualTo(4);
                assertThat(columnOptions.minWriteBufferNumberToMerge()).isEqualTo(2);
                assertThat(columnOptions.writeBufferSize()).isEqualTo(64 * SizeUnit.MB);

                BlockBasedTableConfig tableConfig =
                        (BlockBasedTableConfig) columnOptions.tableFormatConfig();
                assertThat(tableConfig.blockSize()).isEqualTo(4 * SizeUnit.KB);
                assertThat(tableConfig.metadataBlockSize()).isEqualTo(8 * SizeUnit.KB);
                assertThat(tableConfig.blockCacheSize()).isEqualTo(512 * SizeUnit.MB);
                assertThat(tableConfig.filterPolicy()).isInstanceOf(BloomFilter.class);
            }
        }
    }

    @Test
    public void testOptionsFactory() throws Exception {
        String checkpointPath = tempFolder.newFolder().toURI().toString();
        RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(checkpointPath);

        // verify that user-defined options factory could be configured via flink-conf.yaml
        Configuration config = new Configuration();
        config.setString(RocksDBOptions.OPTIONS_FACTORY.key(), TestOptionsFactory.class.getName());
        config.setString(TestOptionsFactory.BACKGROUND_JOBS_OPTION.key(), "4");

        rocksDbBackend = rocksDbBackend.configure(config, getClass().getClassLoader());

        assertThat(rocksDbBackend.getRocksDBOptions()).isInstanceOf(TestOptionsFactory.class);

        try (RocksDBResourceContainer optionsContainer =
                rocksDbBackend.createOptionsAndResourceContainer()) {
            DBOptions dbOptions = optionsContainer.getDbOptions();
            assertThat(dbOptions.maxBackgroundJobs()).isEqualTo(4);
        }

        // verify that user-defined options factory could be set programmatically and override
        // pre-configured one.
        rocksDbBackend.setRocksDBOptions(
                new RocksDBOptionsFactory() {
                    @Override
                    public DBOptions createDBOptions(
                            DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
                        return currentOptions;
                    }

                    @Override
                    public ColumnFamilyOptions createColumnOptions(
                            ColumnFamilyOptions currentOptions,
                            Collection<AutoCloseable> handlesToClose) {
                        return currentOptions.setCompactionStyle(CompactionStyle.FIFO);
                    }
                });

        try (RocksDBResourceContainer optionsContainer =
                rocksDbBackend.createOptionsAndResourceContainer()) {
            ColumnFamilyOptions colCreated = optionsContainer.getColumnOptions();
            assertThat(colCreated.compactionStyle()).isEqualTo(CompactionStyle.FIFO);
        }
    }

    @Test
    public void testPredefinedAndConfigurableOptions() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(RocksDBConfigurableOptions.COMPACTION_STYLE, CompactionStyle.UNIVERSAL);
        try (final RocksDBResourceContainer optionsContainer =
                new RocksDBResourceContainer(
                        configuration, PredefinedOptions.SPINNING_DISK_OPTIMIZED, null, null)) {

            final ColumnFamilyOptions columnFamilyOptions = optionsContainer.getColumnOptions();
            assertThat(columnFamilyOptions).isNotNull();
            assertThat(columnFamilyOptions.compactionStyle()).isEqualTo(CompactionStyle.UNIVERSAL);
        }

        try (final RocksDBResourceContainer optionsContainer =
                new RocksDBResourceContainer(
                        new Configuration(),
                        PredefinedOptions.SPINNING_DISK_OPTIMIZED,
                        null,
                        null)) {

            final ColumnFamilyOptions columnFamilyOptions = optionsContainer.getColumnOptions();
            assertThat(columnFamilyOptions).isNotNull();
            assertThat(columnFamilyOptions.compactionStyle()).isEqualTo(CompactionStyle.LEVEL);
        }
    }

    @Test
    public void testPredefinedAndOptionsFactory() throws Exception {
        final RocksDBOptionsFactory optionsFactory =
                new RocksDBOptionsFactory() {
                    @Override
                    public DBOptions createDBOptions(
                            DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
                        return currentOptions;
                    }

                    @Override
                    public ColumnFamilyOptions createColumnOptions(
                            ColumnFamilyOptions currentOptions,
                            Collection<AutoCloseable> handlesToClose) {
                        return currentOptions.setCompactionStyle(CompactionStyle.UNIVERSAL);
                    }
                };

        try (final RocksDBResourceContainer optionsContainer =
                new RocksDBResourceContainer(
                        PredefinedOptions.SPINNING_DISK_OPTIMIZED, optionsFactory)) {

            final ColumnFamilyOptions columnFamilyOptions = optionsContainer.getColumnOptions();
            assertThat(columnFamilyOptions).isNotNull();
            assertThat(columnFamilyOptions.compactionStyle()).isEqualTo(CompactionStyle.UNIVERSAL);
        }
    }

    // ------------------------------------------------------------------------
    //  Reconfiguration
    // ------------------------------------------------------------------------

    @Test
    public void testRocksDbReconfigurationCopiesExistingValues() throws Exception {
        final FsStateBackend checkpointBackend =
                new FsStateBackend(tempFolder.newFolder().toURI().toString());
        final boolean incremental = !CheckpointingOptions.INCREMENTAL_CHECKPOINTS.defaultValue();

        final RocksDBStateBackend original =
                new RocksDBStateBackend(checkpointBackend, incremental);

        // these must not be the default options
        final PredefinedOptions predOptions = PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM;
        assertThat(original.getPredefinedOptions()).isEqualTo(predOptions);
        original.setPredefinedOptions(predOptions);

        final RocksDBOptionsFactory optionsFactory = mock(RocksDBOptionsFactory.class);
        original.setRocksDBOptions(optionsFactory);

        final String[] localDirs =
                new String[] {
                    tempFolder.newFolder().getAbsolutePath(),
                    tempFolder.newFolder().getAbsolutePath()
                };
        original.setDbStoragePaths(localDirs);

        RocksDBStateBackend copy =
                original.configure(
                        new Configuration(), Thread.currentThread().getContextClassLoader());

        assertThat(copy.isIncrementalCheckpointsEnabled())
                .isEqualTo(original.isIncrementalCheckpointsEnabled());
        assertThat(copy.getDbStoragePaths()).isEqualTo(original.getDbStoragePaths());
        assertThat(copy.getRocksDBOptions()).isEqualTo(original.getRocksDBOptions());
        assertThat(copy.getPredefinedOptions()).isEqualTo(original.getPredefinedOptions());

        FsStateBackend copyCheckpointBackend = (FsStateBackend) copy.getCheckpointBackend();
        assertThat(copyCheckpointBackend.getCheckpointPath())
                .isEqualTo(checkpointBackend.getCheckpointPath());
        assertThat(copyCheckpointBackend.getSavepointPath())
                .isEqualTo(checkpointBackend.getSavepointPath());
    }

    // ------------------------------------------------------------------------
    //  RocksDB Memory Control
    // ------------------------------------------------------------------------

    @Test
    public void testDefaultMemoryControlParameters() {
        RocksDBMemoryConfiguration memSettings = new RocksDBMemoryConfiguration();
        assertThat(memSettings.isUsingManagedMemory()).isTrue();
        assertThat(memSettings.isUsingFixedMemoryPerSlot()).isFalse();
        assertThat(memSettings.getHighPriorityPoolRatio())
                .isEqualTo(RocksDBOptions.HIGH_PRIORITY_POOL_RATIO.defaultValue());
        assertThat(memSettings.getWriteBufferRatio())
                .isEqualTo(RocksDBOptions.WRITE_BUFFER_RATIO.defaultValue());

        RocksDBMemoryConfiguration configured =
                RocksDBMemoryConfiguration.fromOtherAndConfiguration(
                        memSettings, new Configuration());
        assertThat(configured.isUsingManagedMemory()).isTrue();
        assertThat(configured.isUsingFixedMemoryPerSlot()).isFalse();
        assertThat(configured.getHighPriorityPoolRatio())
                .isEqualTo(RocksDBOptions.HIGH_PRIORITY_POOL_RATIO.defaultValue());
        assertThat(configured.getWriteBufferRatio())
                .isEqualTo(RocksDBOptions.WRITE_BUFFER_RATIO.defaultValue());
    }

    @Test
    public void testConfigureManagedMemory() {
        final Configuration config = new Configuration();
        config.setBoolean(RocksDBOptions.USE_MANAGED_MEMORY, true);

        final RocksDBMemoryConfiguration memSettings =
                RocksDBMemoryConfiguration.fromOtherAndConfiguration(
                        new RocksDBMemoryConfiguration(), config);

        assertThat(memSettings.isUsingManagedMemory()).isTrue();
    }

    @Test
    public void testConfigureIllegalMemoryControlParameters() {
        RocksDBMemoryConfiguration memSettings = new RocksDBMemoryConfiguration();

        verifySetParameter(() -> memSettings.setFixedMemoryPerSlot("-1B"));
        verifySetParameter(() -> memSettings.setHighPriorityPoolRatio(-0.1));
        verifySetParameter(() -> memSettings.setHighPriorityPoolRatio(1.1));
        verifySetParameter(() -> memSettings.setWriteBufferRatio(-0.1));
        verifySetParameter(() -> memSettings.setWriteBufferRatio(1.1));

        memSettings.setFixedMemoryPerSlot("128MB");
        memSettings.setWriteBufferRatio(0.6);
        memSettings.setHighPriorityPoolRatio(0.6);

        try {
            // sum of writeBufferRatio and highPriPoolRatio larger than 1.0
            memSettings.validate();
            fail("Expected an IllegalArgumentException.");
        } catch (IllegalArgumentException expected) {
            // expected exception
        }
    }

    private void verifySetParameter(Runnable setter) {
        try {
            setter.run();
            fail("No expected IllegalArgumentException.");
        } catch (IllegalArgumentException expected) {
            // expected exception
        }
    }

    // ------------------------------------------------------------------------
    //  Contained Non-partitioned State Backend
    // ------------------------------------------------------------------------

    @Test
    public void testCallsForwardedToNonPartitionedBackend() throws Exception {
        StateBackend storageBackend = new MemoryStateBackend();
        RocksDBStateBackend rocksDbBackend = new RocksDBStateBackend(storageBackend);
        assertThat(rocksDbBackend.getCheckpointBackend()).isEqualTo(storageBackend);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    static MockEnvironment getMockEnvironment(File... tempDirs) {
        final String[] tempDirStrings = new String[tempDirs.length];
        for (int i = 0; i < tempDirs.length; i++) {
            tempDirStrings[i] = tempDirs[i].getAbsolutePath();
        }

        IOManager ioMan = mock(IOManager.class);
        when(ioMan.getSpillingDirectories()).thenReturn(tempDirs);

        return MockEnvironment.builder()
                .setUserCodeClassLoader(RocksDBStateBackendConfigTest.class.getClassLoader())
                .setTaskManagerRuntimeInfo(
                        new TestingTaskManagerRuntimeInfo(new Configuration(), tempDirStrings))
                .setIOManager(ioMan)
                .build();
    }

    private void verifyIllegalArgument(ConfigOption<?> configOption, String configValue) {
        Configuration configuration = new Configuration();
        configuration.setString(configOption.key(), configValue);

        EmbeddedRocksDBStateBackend stateBackend = new EmbeddedRocksDBStateBackend();
        try {
            stateBackend.configure(configuration, null);
            fail("Not throwing expected IllegalArgumentException.");
        } catch (IllegalArgumentException e) {
            // ignored
        }
    }

    /** An implementation of options factory for testing. */
    public static class TestOptionsFactory implements ConfigurableRocksDBOptionsFactory {
        public static final ConfigOption<Integer> BACKGROUND_JOBS_OPTION =
                ConfigOptions.key("my.custom.rocksdb.backgroundJobs").intType().defaultValue(2);

        private int backgroundJobs = BACKGROUND_JOBS_OPTION.defaultValue();

        @Override
        public DBOptions createDBOptions(
                DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
            return currentOptions.setMaxBackgroundJobs(backgroundJobs);
        }

        @Override
        public ColumnFamilyOptions createColumnOptions(
                ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
            return currentOptions.setCompactionStyle(CompactionStyle.UNIVERSAL);
        }

        @Override
        public RocksDBOptionsFactory configure(ReadableConfig configuration) {
            this.backgroundJobs = configuration.get(BACKGROUND_JOBS_OPTION);
            return this;
        }
    }
}
