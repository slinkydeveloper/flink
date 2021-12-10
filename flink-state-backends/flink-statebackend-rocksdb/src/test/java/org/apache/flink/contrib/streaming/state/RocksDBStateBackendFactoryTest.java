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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.util.TernaryBoolean;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the RocksDBStateBackendFactory. */
public class RocksDBStateBackendFactoryTest {

    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    private final ClassLoader cl = getClass().getClassLoader();

    private final String backendKey = StateBackendOptions.STATE_BACKEND.key();

    // ------------------------------------------------------------------------

    @Test
    public void testFactoryName() {
        // construct the name such that it will not be automatically adjusted on refactorings
        String factoryName = "org.apache.flink.contrib.streaming.state.Roc";
        factoryName += "ksDBStateBackendFactory";

        // !!! if this fails, the code in StateBackendLoader must be adjusted
        assertThat(RocksDBStateBackendFactory.class.getName()).isEqualTo(factoryName);
    }

    @Test
    public void testEmbeddedFactoryName() {
        // construct the name such that it will not be automatically adjusted on refactorings
        String factoryName = "org.apache.flink.contrib.streaming.state.EmbeddedRoc";
        factoryName += "ksDBStateBackendFactory";

        // !!! if this fails, the code in StateBackendLoader must be adjusted
        assertThat(EmbeddedRocksDBStateBackendFactory.class.getName()).isEqualTo(factoryName);
    }

    /**
     * Validates loading a file system state backend with additional parameters from the cluster
     * configuration.
     */
    @Test
    public void testLoadRocksDBStateBackend() throws Exception {
        final String localDir1 = tmp.newFolder().getAbsolutePath();
        final String localDir2 = tmp.newFolder().getAbsolutePath();
        final String localDirs = localDir1 + File.pathSeparator + localDir2;
        final boolean incremental = !CheckpointingOptions.INCREMENTAL_CHECKPOINTS.defaultValue();

        // we configure with the explicit string (rather than
        // AbstractStateBackend#X_STATE_BACKEND_NAME)
        // to guard against config-breaking changes of the name
        final Configuration config1 = new Configuration();
        config1.setString(backendKey, "rocksdb");
        config1.setString(RocksDBOptions.LOCAL_DIRECTORIES, localDirs);
        config1.setBoolean(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, incremental);

        final Configuration config2 = new Configuration();
        config2.setString(backendKey, EmbeddedRocksDBStateBackendFactory.class.getName());
        config2.setString(RocksDBOptions.LOCAL_DIRECTORIES, localDirs);
        config2.setBoolean(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, incremental);

        StateBackend backend1 = StateBackendLoader.loadStateBackendFromConfig(config1, cl, null);
        StateBackend backend2 = StateBackendLoader.loadStateBackendFromConfig(config2, cl, null);

        assertThat(backend1).isInstanceOf(EmbeddedRocksDBStateBackend.class);
        assertThat(backend2).isInstanceOf(EmbeddedRocksDBStateBackend.class);

        EmbeddedRocksDBStateBackend fs1 = (EmbeddedRocksDBStateBackend) backend1;
        EmbeddedRocksDBStateBackend fs2 = (EmbeddedRocksDBStateBackend) backend2;

        assertThat(fs1.isIncrementalCheckpointsEnabled()).isEqualTo(incremental);
        assertThat(fs2.isIncrementalCheckpointsEnabled()).isEqualTo(incremental);
        checkPaths(fs1.getDbStoragePaths(), localDir1, localDir2);
        checkPaths(fs2.getDbStoragePaths(), localDir1, localDir2);
    }

    /**
     * Validates taking the application-defined rocksdb state backend and adding with additional
     * parameters from the cluster configuration, but giving precedence to application-defined
     * parameters over configuration-defined parameters.
     */
    @Test
    public void testLoadRocksDBStateBackendMixed() throws Exception {
        final String localDir1 = tmp.newFolder().getAbsolutePath();
        final String localDir2 = tmp.newFolder().getAbsolutePath();
        final String localDir3 = tmp.newFolder().getAbsolutePath();
        final String localDir4 = tmp.newFolder().getAbsolutePath();

        final boolean incremental = !CheckpointingOptions.INCREMENTAL_CHECKPOINTS.defaultValue();

        final EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend(incremental);
        backend.setDbStoragePaths(localDir1, localDir2);

        final Configuration config = new Configuration();
        config.setString(backendKey, "hashmap"); // this should not be picked up
        config.setBoolean(
                CheckpointingOptions.INCREMENTAL_CHECKPOINTS,
                !incremental); // this should not be picked up
        config.setString(
                RocksDBOptions.LOCAL_DIRECTORIES,
                localDir3 + ":" + localDir4); // this should not be picked up

        final StateBackend loadedBackend =
                StateBackendLoader.fromApplicationOrConfigOrDefault(
                        backend, TernaryBoolean.UNDEFINED, config, cl, null);
        assertThat(loadedBackend).isInstanceOf(EmbeddedRocksDBStateBackend.class);

        final EmbeddedRocksDBStateBackend loadedRocks = (EmbeddedRocksDBStateBackend) loadedBackend;

        assertThat(loadedRocks.isIncrementalCheckpointsEnabled()).isEqualTo(incremental);
        checkPaths(loadedRocks.getDbStoragePaths(), localDir1, localDir2);
    }

    // ------------------------------------------------------------------------

    private static void checkPaths(String[] pathsArray, String... paths) {
        assertThat(pathsArray).isNotNull();
        assertThat(paths).isNotNull();

        assertThat(paths.length).isEqualTo(pathsArray.length);

        HashSet<String> pathsSet = new HashSet<>(Arrays.asList(pathsArray));

        for (String path : paths) {
            assertThat(pathsSet.contains(path)).isTrue();
        }
    }
}
