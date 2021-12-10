/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks.mailbox;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.function.FutureTaskWithException;
import org.apache.flink.util.function.RunnableWithException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Unit tests for {@link MailboxProcessor}. */
public class TaskMailboxProcessorTest {

    public static final int DEFAULT_PRIORITY = 0;

    @Rule public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testRejectIfNotOpen() {
        MailboxProcessor mailboxProcessor = new MailboxProcessor(controller -> {});
        mailboxProcessor.prepareClose();
        try {
            mailboxProcessor.getMailboxExecutor(DEFAULT_PRIORITY).execute(() -> {}, "dummy");
            fail("Should not be able to accept runnables if not opened.");
        } catch (RejectedExecutionException expected) {
        }
    }

    @Test
    public void testSubmittingRunnableWithException() throws Exception {
        expectedException.expectMessage("Expected");
        try (MailboxProcessor mailboxProcessor = new MailboxProcessor(controller -> {})) {
            final Thread submitThread =
                    new Thread(
                            () -> {
                                mailboxProcessor
                                        .getMainMailboxExecutor()
                                        .execute(
                                                this::throwFlinkException,
                                                "testSubmittingRunnableWithException");
                            });

            submitThread.start();
            mailboxProcessor.runMailboxLoop();
            submitThread.join();
        }
    }

    private void throwFlinkException() throws FlinkException {
        throw new FlinkException("Expected");
    }

    @Test
    public void testShutdown() {
        MailboxProcessor mailboxProcessor = new MailboxProcessor(controller -> {});
        FutureTaskWithException<Void> testRunnableFuture = new FutureTaskWithException<>(() -> {});
        mailboxProcessor
                .getMailboxExecutor(DEFAULT_PRIORITY)
                .execute(testRunnableFuture, "testRunnableFuture");
        mailboxProcessor.prepareClose();

        try {
            mailboxProcessor.getMailboxExecutor(DEFAULT_PRIORITY).execute(() -> {}, "dummy");
            fail("Should not be able to accept runnables if not opened.");
        } catch (RejectedExecutionException expected) {
        }

        assertThat(testRunnableFuture.isDone()).isFalse();

        mailboxProcessor.close();
        assertThat(testRunnableFuture.isCancelled()).isTrue();
    }

    @Test
    public void testRunDefaultActionAndMails() throws Exception {
        AtomicBoolean stop = new AtomicBoolean(false);
        MailboxThread mailboxThread =
                new MailboxThread() {
                    @Override
                    public void runDefaultAction(Controller controller) throws Exception {
                        if (stop.get()) {
                            controller.allActionsCompleted();
                        } else {
                            Thread.sleep(10L);
                        }
                    }
                };

        MailboxProcessor mailboxProcessor = start(mailboxThread);
        mailboxProcessor.getMailboxExecutor(DEFAULT_PRIORITY).execute(() -> stop.set(true), "stop");
        mailboxThread.join();
    }

    @Test
    public void testRunDefaultAction() throws Exception {

        final int expectedInvocations = 3;
        final AtomicInteger counter = new AtomicInteger(0);
        MailboxThread mailboxThread =
                new MailboxThread() {
                    @Override
                    public void runDefaultAction(Controller controller) {
                        if (counter.incrementAndGet() == expectedInvocations) {
                            controller.allActionsCompleted();
                        }
                    }
                };

        start(mailboxThread);
        mailboxThread.join();
        assertThat(counter.get()).isEqualTo(expectedInvocations);
    }

    @Test
    public void testSignalUnAvailable() throws Exception {

        final AtomicInteger counter = new AtomicInteger(0);
        final AtomicReference<MailboxDefaultAction.Suspension> suspendedActionRef =
                new AtomicReference<>();
        final OneShotLatch actionSuspendedLatch = new OneShotLatch();
        final int blockAfterInvocations = 3;
        final int totalInvocations = blockAfterInvocations * 2;

        MailboxThread mailboxThread =
                new MailboxThread() {
                    @Override
                    public void runDefaultAction(Controller controller) {
                        if (counter.incrementAndGet() == blockAfterInvocations) {
                            suspendedActionRef.set(controller.suspendDefaultAction());
                            actionSuspendedLatch.trigger();
                        } else if (counter.get() == totalInvocations) {
                            controller.allActionsCompleted();
                        }
                    }
                };

        MailboxProcessor mailboxProcessor = start(mailboxThread);
        actionSuspendedLatch.await();
        assertThat(counter.get()).isEqualTo(blockAfterInvocations);

        MailboxDefaultAction.Suspension suspension = suspendedActionRef.get();
        mailboxProcessor.getMailboxExecutor(DEFAULT_PRIORITY).execute(suspension::resume, "resume");
        mailboxThread.join();
        assertThat(counter.get()).isEqualTo(totalInvocations);
    }

    @Test
    public void testSignalUnAvailablePingPong() throws Exception {
        final AtomicReference<MailboxDefaultAction.Suspension> suspendedActionRef =
                new AtomicReference<>();
        final int totalSwitches = 10000;
        final MailboxThread mailboxThread =
                new MailboxThread() {
                    int count = 0;

                    @Override
                    public void runDefaultAction(Controller controller) {

                        // If this is violated, it means that the default action was invoked while
                        // we assumed suspension
                        assertThat(
                                        suspendedActionRef.compareAndSet(
                                                null, controller.suspendDefaultAction()))
                                .isTrue();

                        ++count;

                        if (count == totalSwitches) {
                            controller.allActionsCompleted();
                        } else if (count % 1000 == 0) {
                            try {
                                Thread.sleep(1L);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                };

        mailboxThread.start();
        final MailboxProcessor mailboxProcessor = mailboxThread.getMailboxProcessor();

        final Thread asyncUnblocker =
                new Thread(
                        () -> {
                            int count = 0;
                            while (!Thread.currentThread().isInterrupted()) {

                                final MailboxDefaultAction.Suspension resume =
                                        suspendedActionRef.getAndSet(null);
                                if (resume != null) {
                                    mailboxProcessor
                                            .getMailboxExecutor(DEFAULT_PRIORITY)
                                            .execute(resume::resume, "resume");
                                } else {
                                    try {
                                        mailboxProcessor
                                                .getMailboxExecutor(DEFAULT_PRIORITY)
                                                .execute(() -> {}, "dummy");
                                    } catch (RejectedExecutionException ignore) {
                                    }
                                }

                                ++count;
                                if (count % 5000 == 0) {
                                    try {
                                        Thread.sleep(1L);
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                    }
                                }
                            }
                        });

        asyncUnblocker.start();
        mailboxThread.signalStart();
        mailboxThread.join();
        asyncUnblocker.interrupt();
        asyncUnblocker.join();
        mailboxThread.checkException();
    }

    /** Testing that canceling after closing will not lead to an exception. */
    @Test
    public void testCancelAfterClose() {
        MailboxProcessor mailboxProcessor = new MailboxProcessor((ctx) -> {});
        mailboxProcessor.close();
        mailboxProcessor.allActionsCompleted();
    }

    private static MailboxProcessor start(MailboxThread mailboxThread) {
        mailboxThread.start();
        final MailboxProcessor mailboxProcessor = mailboxThread.getMailboxProcessor();
        mailboxThread.signalStart();
        return mailboxProcessor;
    }

    /** FLINK-14304: Avoid newly spawned letters to prevent input processing from ever happening. */
    @Test
    public void testAvoidStarvation() throws Exception {

        final int expectedInvocations = 3;
        final AtomicInteger counter = new AtomicInteger(0);
        MailboxThread mailboxThread =
                new MailboxThread() {
                    @Override
                    public void runDefaultAction(Controller controller) {
                        if (counter.incrementAndGet() == expectedInvocations) {
                            controller.allActionsCompleted();
                        }
                    }
                };

        mailboxThread.start();
        final MailboxProcessor mailboxProcessor = mailboxThread.getMailboxProcessor();
        final MailboxExecutor mailboxExecutor =
                mailboxProcessor.getMailboxExecutor(DEFAULT_PRIORITY);
        AtomicInteger index = new AtomicInteger();
        mailboxExecutor.execute(
                new RunnableWithException() {
                    @Override
                    public void run() {
                        mailboxExecutor.execute(this, "Blocking mail" + index.incrementAndGet());
                    }
                },
                "Blocking mail" + index.get());

        mailboxThread.signalStart();
        mailboxThread.join();

        assertThat(counter.get()).isEqualTo(expectedInvocations);
        assertThat(index.get()).isEqualTo(expectedInvocations);
    }

    @Test
    public void testSuspendRunningMailboxLoop() throws Exception {
        // given: Thread for suspending the suspendable loop.
        OneShotLatch doSomeWork = new OneShotLatch();
        AtomicBoolean stop = new AtomicBoolean(false);

        MailboxProcessor mailboxProcessor =
                new MailboxProcessor(
                        controller -> {
                            doSomeWork.trigger();

                            if (stop.get()) {
                                controller.allActionsCompleted();
                            }
                        });

        Thread suspendThread =
                new Thread(
                        () -> {
                            try {
                                // Ensure that loop was started.
                                doSomeWork.await();

                                // when: Suspend the suspendable loop.
                                mailboxProcessor.suspend();

                                // and: Execute the command for stopping the loop.
                                mailboxProcessor
                                        .getMailboxExecutor(DEFAULT_PRIORITY)
                                        .execute(() -> stop.set(true), "stop");
                            } catch (Exception ignore) {

                            }
                        });
        suspendThread.start();

        // when: Start the suspendable loop.
        mailboxProcessor.runMailboxLoop();

        suspendThread.join();

        // then: Mailbox is not stopped because it was suspended before the stop command.
        assertThat(stop.get()).isFalse();

        // when: Resume the suspendable loop.
        mailboxProcessor.runMailboxLoop();

        // then: Stop command successfully executed because it was in the queue.
        assertThat(mailboxProcessor.isMailboxLoopRunning()).isFalse();
        assertThat(stop.get()).isTrue();
    }

    @Test
    public void testResumeMailboxLoopAfterAllActionsCompleted() throws Exception {
        // given: Configured suspendable loop.
        AtomicBoolean start = new AtomicBoolean(false);
        MailboxProcessor mailboxProcessor = new MailboxProcessor(controller -> start.set(true));

        // when: Complete mailbox loop immediately after start.
        mailboxProcessor.allActionsCompleted();
        mailboxProcessor.runMailboxLoop();

        // then: Mailbox is finished without execution any job.
        assertThat(mailboxProcessor.isMailboxLoopRunning()).isFalse();
        assertThat(start.get()).isFalse();

        // when: Start the suspendable loop again.
        mailboxProcessor.runMailboxLoop();

        // then: Nothing happens because mailbox is already permanently finished.
        assertThat(start.get()).isFalse();
    }

    @Test
    public void testResumeMailboxLoop() throws Exception {
        // given: Configured suspendable loop.
        AtomicBoolean start = new AtomicBoolean(false);
        MailboxProcessor mailboxProcessor =
                new MailboxProcessor(
                        controller -> {
                            start.set(true);

                            controller.allActionsCompleted();
                        });

        // when: Suspend mailbox loop immediately after start.
        mailboxProcessor.suspend();
        mailboxProcessor.runMailboxLoop();

        // then: Mailbox is not finished but job didn't execute because it was suspended.
        assertThat(start.get()).isFalse();

        // when: Start the suspendable loop again.
        mailboxProcessor.runMailboxLoop();

        // then: Job is done.
        assertThat(start.get()).isTrue();
    }

    static class MailboxThread extends Thread implements MailboxDefaultAction {

        MailboxProcessor mailboxProcessor;
        OneShotLatch mailboxCreatedLatch = new OneShotLatch();
        OneShotLatch canRun = new OneShotLatch();
        private Throwable caughtException;

        @Override
        public final void run() {
            mailboxProcessor = new MailboxProcessor(this);
            mailboxCreatedLatch.trigger();
            try {
                canRun.await();
                mailboxProcessor.runMailboxLoop();
            } catch (Throwable t) {
                this.caughtException = t;
            }
        }

        @Override
        public void runDefaultAction(Controller controller) throws Exception {
            controller.allActionsCompleted();
        }

        final MailboxProcessor getMailboxProcessor() {
            try {
                mailboxCreatedLatch.await();
                return mailboxProcessor;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        final void signalStart() {
            if (mailboxCreatedLatch.isTriggered()) {
                canRun.trigger();
            }
        }

        void checkException() throws Exception {
            if (caughtException != null) {
                throw new Exception(caughtException);
            }
        }
    }
}
