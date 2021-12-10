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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.NeverCompleteFuture;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatObject;

/** Tests for {@link ProcessingTimeServiceImpl}. */
public class ProcessingTimeServiceImplTest extends TestLogger {

    private static final Time testingTimeout = Time.seconds(10L);

    private SystemProcessingTimeService timerService;

    @Before
    public void setup() {
        CompletableFuture<Throwable> errorFuture = new CompletableFuture<>();

        timerService = new SystemProcessingTimeService(errorFuture::complete);
    }

    @After
    public void teardown() {
        timerService.shutdownService();
    }

    @Test
    public void testTimerRegistrationAndCancellation()
            throws TimeoutException, InterruptedException, ExecutionException {
        ProcessingTimeServiceImpl processingTimeService =
                new ProcessingTimeServiceImpl(timerService, v -> v);

        // test registerTimer() and cancellation
        ScheduledFuture<?> neverFiredTimer =
                processingTimeService.registerTimer(Long.MAX_VALUE, timestamp -> {});
        assertThat(timerService.getNumTasksScheduled()).isEqualTo(1);
        assertThat(neverFiredTimer.cancel(false)).isTrue();
        assertThat(neverFiredTimer.isDone()).isTrue();
        assertThat(neverFiredTimer.isCancelled()).isTrue();

        final CompletableFuture<?> firedTimerFuture = new CompletableFuture<>();
        ScheduledFuture<?> firedTimer =
                processingTimeService.registerTimer(
                        0, timestamp -> firedTimerFuture.complete(null));
        firedTimer.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
        assertThat(firedTimerFuture.isDone()).isTrue();
        assertThat(firedTimer.isCancelled()).isFalse();

        // test scheduleAtFixedRate() and cancellation
        final CompletableFuture<?> periodicTimerFuture = new CompletableFuture<>();
        ScheduledFuture<?> periodicTimer =
                processingTimeService.scheduleAtFixedRate(
                        timestamp -> periodicTimerFuture.complete(null), 0, Long.MAX_VALUE);

        periodicTimerFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
        assertThat(periodicTimer.cancel(false)).isTrue();
        assertThat(periodicTimer.isDone()).isTrue();
        assertThat(periodicTimer.isCancelled()).isTrue();
    }

    @Test
    public void testQuiesce() throws Exception {
        ProcessingTimeServiceImpl processingTimeService =
                new ProcessingTimeServiceImpl(timerService, v -> v);

        final CompletableFuture<?> timerRunFuture = new CompletableFuture();
        final OneShotLatch timerWaitLatch = new OneShotLatch();

        ScheduledFuture<?> timer =
                processingTimeService.registerTimer(
                        0,
                        timestamp -> {
                            timerRunFuture.complete(null);
                            timerWaitLatch.await();
                        });

        // wait for the timer to run, then quiesce the time service
        timerRunFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
        CompletableFuture<?> quiesceCompletedFuture = processingTimeService.quiesce();

        // after the timer server is quiesced, tests #registerTimer() and #scheduleAtFixedRate()
        assertThatObject(processingTimeService.registerTimer(0, timestamp -> {}))
                .isInstanceOf(NeverCompleteFuture.class);
        assertThatObject(
                        processingTimeService.scheduleAtFixedRate(
                                timestamp -> {}, 0, Long.MAX_VALUE))
                .isInstanceOf(NeverCompleteFuture.class);

        // when the timer is finished, the quiesce-completed future should be completed
        assertThat(quiesceCompletedFuture.isDone()).isFalse();
        timerWaitLatch.trigger();
        timer.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
        assertThat(quiesceCompletedFuture.isDone()).isTrue();
    }

    @Test
    public void testQuiesceWhenNoRunningTimers() {
        ProcessingTimeServiceImpl processingTimeService =
                new ProcessingTimeServiceImpl(timerService, v -> v);
        assertThat(processingTimeService.quiesce().isDone()).isTrue();
    }
}
