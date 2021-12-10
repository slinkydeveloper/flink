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

package org.apache.flink.util.concurrent;

import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FixedRetryStrategy}. */
public class FixedRetryStrategyTest extends TestLogger {

    @Test
    public void testGetters() throws Exception {
        RetryStrategy retryStrategy = new FixedRetryStrategy(10, Duration.ofMillis(5L));
        assertThat(retryStrategy.getNumRemainingRetries()).isEqualTo(10);
        assertThat(retryStrategy.getRetryDelay()).isEqualTo(Duration.ofMillis(5L));

        RetryStrategy nextRetryStrategy = retryStrategy.getNextRetryStrategy();
        assertThat(nextRetryStrategy.getNumRemainingRetries()).isEqualTo(9);
        assertThat(nextRetryStrategy.getRetryDelay()).isEqualTo(Duration.ofMillis(5L));
    }

    /** Tests that getting a next RetryStrategy below zero remaining retries fails. */
    @Test(expected = IllegalStateException.class)
    public void testRetryFailure() throws Throwable {
        new FixedRetryStrategy(0, Duration.ofMillis(5L)).getNextRetryStrategy();
    }
}
