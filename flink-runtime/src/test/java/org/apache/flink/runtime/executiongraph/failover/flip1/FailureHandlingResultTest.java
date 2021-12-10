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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link FailureHandlingResult}. */
public class FailureHandlingResultTest extends TestLogger {

    /** Tests normal FailureHandlingResult. */
    @Test
    public void testNormalFailureHandlingResult() {
        // create a normal FailureHandlingResult
        ExecutionVertexID executionVertexID = new ExecutionVertexID(new JobVertexID(), 0);
        Set<ExecutionVertexID> tasks = new HashSet<>();
        tasks.add(executionVertexID);
        long delay = 1234;
        Throwable error = new RuntimeException();
        long timestamp = System.currentTimeMillis();
        FailureHandlingResult result =
                FailureHandlingResult.restartable(
                        executionVertexID, error, timestamp, tasks, delay, false);

        assertThat(result.canRestart()).isTrue();
        assertThat(result.getRestartDelayMS()).isEqualTo(delay);
        assertThat(result.getVerticesToRestart()).isEqualTo(tasks);
        assertThat(result.getError()).isSameAs(error);
        assertThat(result.getTimestamp()).isEqualTo(timestamp);
        assertThat(result.getExecutionVertexIdOfFailedTask().isPresent()).isTrue();
        assertThat(result.getExecutionVertexIdOfFailedTask().get()).isEqualTo(executionVertexID);
    }

    /** Tests FailureHandlingResult which suppresses restarts. */
    @Test
    public void testRestartingSuppressedFailureHandlingResultWithNoCausingExecutionVertexId() {
        // create a FailureHandlingResult with error
        Throwable error = new Exception("test error");
        long timestamp = System.currentTimeMillis();
        FailureHandlingResult result =
                FailureHandlingResult.unrecoverable(null, error, timestamp, false);

        assertThat(result.canRestart()).isFalse();
        assertThat(result.getError()).isSameAs(error);
        assertThat(result.getTimestamp()).isEqualTo(timestamp);
        assertThat(result.getExecutionVertexIdOfFailedTask().isPresent()).isFalse();
        try {
            result.getVerticesToRestart();
            fail("get tasks to restart is not allowed when restarting is suppressed");
        } catch (IllegalStateException ex) {
            // expected
        }
        try {
            result.getRestartDelayMS();
            fail("get restart delay is not allowed when restarting is suppressed");
        } catch (IllegalStateException ex) {
            // expected
        }
    }
}
