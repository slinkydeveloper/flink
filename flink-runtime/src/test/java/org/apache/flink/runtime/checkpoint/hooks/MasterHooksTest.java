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

package org.apache.flink.runtime.checkpoint.hooks;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nullable;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Tests for the MasterHooks utility class. */
public class MasterHooksTest extends TestLogger {

    // ------------------------------------------------------------------------
    //  hook management
    // ------------------------------------------------------------------------

    @Test
    public void wrapHook() throws Exception {
        final String id = "id";

        Thread thread = Thread.currentThread();
        final ClassLoader originalClassLoader = thread.getContextClassLoader();
        final ClassLoader userClassLoader = new URLClassLoader(new URL[0]);

        final Runnable command =
                spy(
                        new Runnable() {

                            @Override
                            public void run() {
                                assertThat(Thread.currentThread().getContextClassLoader())
                                        .isEqualTo(userClassLoader);
                            }
                        });

        MasterTriggerRestoreHook<String> hook =
                spy(
                        new MasterTriggerRestoreHook<String>() {

                            @Override
                            public String getIdentifier() {
                                assertThat(Thread.currentThread().getContextClassLoader())
                                        .isEqualTo(userClassLoader);
                                return id;
                            }

                            @Override
                            public void reset() throws Exception {
                                assertThat(Thread.currentThread().getContextClassLoader())
                                        .isEqualTo(userClassLoader);
                            }

                            @Override
                            public void close() throws Exception {
                                assertThat(Thread.currentThread().getContextClassLoader())
                                        .isEqualTo(userClassLoader);
                            }

                            @Nullable
                            @Override
                            public CompletableFuture<String> triggerCheckpoint(
                                    long checkpointId, long timestamp, Executor executor)
                                    throws Exception {
                                assertThat(Thread.currentThread().getContextClassLoader())
                                        .isEqualTo(userClassLoader);
                                executor.execute(command);
                                return null;
                            }

                            @Override
                            public void restoreCheckpoint(
                                    long checkpointId, @Nullable String checkpointData)
                                    throws Exception {
                                assertThat(Thread.currentThread().getContextClassLoader())
                                        .isEqualTo(userClassLoader);
                            }

                            @Nullable
                            @Override
                            public SimpleVersionedSerializer<String>
                                    createCheckpointDataSerializer() {
                                assertThat(Thread.currentThread().getContextClassLoader())
                                        .isEqualTo(userClassLoader);
                                return null;
                            }
                        });

        MasterTriggerRestoreHook<String> wrapped = MasterHooks.wrapHook(hook, userClassLoader);

        // verify getIdentifier
        wrapped.getIdentifier();
        verify(hook, times(1)).getIdentifier();
        assertThat(thread.getContextClassLoader()).isEqualTo(originalClassLoader);

        // verify triggerCheckpoint and its wrapped executor
        TestExecutor testExecutor = new TestExecutor();
        wrapped.triggerCheckpoint(0L, 0, testExecutor);
        assertThat(thread.getContextClassLoader()).isEqualTo(originalClassLoader);
        assertThat(testExecutor.command).isNotNull();
        testExecutor.command.run();
        verify(command, times(1)).run();
        assertThat(thread.getContextClassLoader()).isEqualTo(originalClassLoader);

        // verify restoreCheckpoint
        wrapped.restoreCheckpoint(0L, "");
        verify(hook, times(1)).restoreCheckpoint(eq(0L), eq(""));
        assertThat(thread.getContextClassLoader()).isEqualTo(originalClassLoader);

        // verify createCheckpointDataSerializer
        wrapped.createCheckpointDataSerializer();
        verify(hook, times(1)).createCheckpointDataSerializer();
        assertThat(thread.getContextClassLoader()).isEqualTo(originalClassLoader);

        // verify close
        wrapped.close();
        verify(hook, times(1)).close();
        assertThat(thread.getContextClassLoader()).isEqualTo(originalClassLoader);
    }

    private static class TestExecutor implements Executor {
        Runnable command;

        @Override
        public void execute(Runnable command) {
            this.command = command;
        }
    }
}
