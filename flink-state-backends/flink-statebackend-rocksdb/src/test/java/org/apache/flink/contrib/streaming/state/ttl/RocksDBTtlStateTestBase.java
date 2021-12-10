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

package org.apache.flink.contrib.streaming.state.ttl;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.ttl.StateBackendTestContext;
import org.apache.flink.runtime.state.ttl.TtlStateTestBase;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TernaryBoolean;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDBException;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Base test suite for rocksdb state TTL. */
public abstract class RocksDBTtlStateTestBase extends TtlStateTestBase {
    @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

    @Override
    protected StateBackendTestContext createStateBackendTestContext(TtlTimeProvider timeProvider) {
        return new StateBackendTestContext(timeProvider) {
            @Override
            protected StateBackend createStateBackend() {
                return RocksDBTtlStateTestBase.this.createStateBackend();
            }
        };
    }

    abstract StateBackend createStateBackend();

    StateBackend createStateBackend(TernaryBoolean enableIncrementalCheckpointing) {
        String dbPath;
        String checkpointPath;
        try {
            dbPath = tempFolder.newFolder().getAbsolutePath();
            checkpointPath = tempFolder.newFolder().toURI().toString();
        } catch (IOException e) {
            throw new FlinkRuntimeException("Failed to init rocksdb test state backend");
        }
        RocksDBStateBackend backend =
                new RocksDBStateBackend(
                        new FsStateBackend(checkpointPath), enableIncrementalCheckpointing);
        Configuration config = new Configuration();
        backend = backend.configure(config, Thread.currentThread().getContextClassLoader());
        backend.setDbStoragePath(dbPath);
        return backend;
    }

    @Test
    public void testCompactFilter() throws Exception {
        testCompactFilter(false, false);
    }

    @Test
    public void testCompactFilterWithSnapshot() throws Exception {
        testCompactFilter(true, false);
    }

    @Test
    public void testCompactFilterWithSnapshotAndRescalingAfterRestore() throws Exception {
        testCompactFilter(true, true);
    }

    @SuppressWarnings("resource")
    private void testCompactFilter(boolean takeSnapshot, boolean rescaleAfterRestore)
            throws Exception {
        int numberOfKeyGroupsAfterRestore = StateBackendTestContext.NUMBER_OF_KEY_GROUPS;
        if (rescaleAfterRestore) {
            numberOfKeyGroupsAfterRestore *= 2;
        }

        StateDescriptor<?, ?> stateDesc =
                initTest(
                        getConfBuilder(TTL)
                                .setStateVisibility(
                                        StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                                .build());

        if (takeSnapshot) {
            takeAndRestoreSnapshot(numberOfKeyGroupsAfterRestore);
        }

        setTimeAndCompact(stateDesc, 0L);

        sbetc.setCurrentKey("k1");
        ctx().update(ctx().updateEmpty);
        checkUnexpiredOriginalAvailable();

        sbetc.setCurrentKey("k2");
        ctx().update(ctx().updateEmpty);
        checkUnexpiredOriginalAvailable();

        if (takeSnapshot) {
            takeAndRestoreSnapshot(numberOfKeyGroupsAfterRestore);
        }

        setTimeAndCompact(stateDesc, 50L);

        sbetc.setCurrentKey("k1");
        checkUnexpiredOriginalAvailable();
        assertThat(ctx().get()).as(UNEXPIRED_AVAIL).isEqualTo(ctx().getUpdateEmpty);

        ctx().update(ctx().updateUnexpired);
        checkUnexpiredOriginalAvailable();

        sbetc.setCurrentKey("k2");
        checkUnexpiredOriginalAvailable();
        assertThat(ctx().get()).as(UNEXPIRED_AVAIL).isEqualTo(ctx().getUpdateEmpty);

        ctx().update(ctx().updateUnexpired);
        checkUnexpiredOriginalAvailable();

        if (takeSnapshot) {
            takeAndRestoreSnapshot(numberOfKeyGroupsAfterRestore);
        }

        // compaction which should not touch unexpired data
        // and merge list element with different expiration time
        setTimeAndCompact(stateDesc, 80L);
        // expire oldest data
        setTimeAndCompact(stateDesc, 120L);

        sbetc.setCurrentKey("k1");
        checkUnexpiredOriginalAvailable();
        assertThat(ctx().get()).as(UPDATED_UNEXPIRED_AVAIL).isEqualTo(ctx().getUnexpired);

        sbetc.setCurrentKey("k2");
        checkUnexpiredOriginalAvailable();
        assertThat(ctx().get()).as(UPDATED_UNEXPIRED_AVAIL).isEqualTo(ctx().getUnexpired);

        if (takeSnapshot) {
            takeAndRestoreSnapshot(numberOfKeyGroupsAfterRestore);
        }

        setTimeAndCompact(stateDesc, 170L);
        sbetc.setCurrentKey("k1");
        assertThat(ctx().isOriginalEmptyValue())
                .as("Expired original state should be unavailable")
                .isTrue();
        assertThat(ctx().get()).as(EXPIRED_UNAVAIL).isEqualTo(ctx().emptyValue);

        sbetc.setCurrentKey("k2");
        assertThat(ctx().isOriginalEmptyValue())
                .as("Expired original state should be unavailable")
                .isTrue();
        assertThat(ctx().get())
                .as("Expired state should be unavailable")
                .isEqualTo(ctx().emptyValue);
    }

    private void checkUnexpiredOriginalAvailable() throws Exception {
        assertThat(ctx().getOriginal())
                .as("Unexpired original state should be available")
                .isEqualTo(ctx().emptyValue);
    }

    private void setTimeAndCompact(StateDescriptor<?, ?> stateDesc, long ts)
            throws RocksDBException {
        @SuppressWarnings("resource")
        RocksDBKeyedStateBackend<String> keyedBackend = sbetc.getKeyedStateBackend();
        timeProvider.time = ts;
        keyedBackend.compactState(stateDesc);
    }
}
