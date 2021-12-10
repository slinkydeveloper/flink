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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.RunnableWithException;

import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.HamcrestCondition.matching;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/** Tests for {@link DefaultLeaderElectionService}. */
public class DefaultLeaderElectionServiceTest extends TestLogger {

    private static final String TEST_URL = "akka//user/jobmanager";
    private static final long timeout = 50L;

    @Test
    public void testOnGrantAndRevokeLeadership() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            // grant leadership
                            testingLeaderElectionDriver.isLeader();
                            testingContender.waitForLeader(timeout);
                            assertThat(testingContender.getDescription()).isEqualTo(TEST_URL);
                            assertThat(testingContender.getLeaderSessionID())
                                    .isEqualTo(leaderElectionService.getLeaderSessionID());
                            // Check the external storage
                            assertThat(testingLeaderElectionDriver.getLeaderInformation())
                                    .isEqualTo(
                                            LeaderInformation.known(
                                                    leaderElectionService.getLeaderSessionID(),
                                                    TEST_URL));
                            // revoke leadership
                            testingLeaderElectionDriver.notLeader();
                            testingContender.waitForRevokeLeader(timeout);
                            assertThat(testingContender.getLeaderSessionID())
                                    .isEqualTo(nullValue());
                            assertThat(leaderElectionService.getLeaderSessionID())
                                    .isEqualTo(nullValue());
                            // External storage should be cleared
                            assertThat(testingLeaderElectionDriver.getLeaderInformation())
                                    .isEqualTo(LeaderInformation.empty());
                        });
            }
        };
    }

    @Test
    public void testLeaderInformationChangedAndShouldBeCorrected() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            testingLeaderElectionDriver.isLeader();
                            final LeaderInformation expectedLeader =
                                    LeaderInformation.known(
                                            leaderElectionService.getLeaderSessionID(), TEST_URL);
                            // Leader information changed on external storage. It should be
                            // corrected.
                            testingLeaderElectionDriver.leaderInformationChanged(
                                    LeaderInformation.empty());
                            assertThat(testingLeaderElectionDriver.getLeaderInformation())
                                    .isEqualTo(expectedLeader);
                            testingLeaderElectionDriver.leaderInformationChanged(
                                    LeaderInformation.known(UUID.randomUUID(), "faulty-address"));
                            assertThat(testingLeaderElectionDriver.getLeaderInformation())
                                    .isEqualTo(expectedLeader);
                        });
            }
        };
    }

    @Test
    public void testHasLeadership() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            testingLeaderElectionDriver.isLeader();
                            final UUID currentLeaderSessionId =
                                    leaderElectionService.getLeaderSessionID();
                            assertThat(currentLeaderSessionId).isEqualTo(notNullValue());
                            assertThat(leaderElectionService.hasLeadership(currentLeaderSessionId))
                                    .isEqualTo(true);
                            assertThat(leaderElectionService.hasLeadership(UUID.randomUUID()))
                                    .isEqualTo(false);
                            leaderElectionService.stop();
                            assertThat(leaderElectionService.hasLeadership(currentLeaderSessionId))
                                    .isEqualTo(false);
                        });
            }
        };
    }

    @Test
    public void testLeaderInformationChangedIfNotBeingLeader() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            final LeaderInformation faultyLeader =
                                    LeaderInformation.known(UUID.randomUUID(), "faulty-address");
                            testingLeaderElectionDriver.leaderInformationChanged(faultyLeader);
                            // External storage should keep the wrong value.
                            assertThat(testingLeaderElectionDriver.getLeaderInformation())
                                    .isEqualTo(faultyLeader);
                        });
            }
        };
    }

    @Test
    public void testOnGrantLeadershipIsIgnoredAfterBeingStop() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderElectionService.stop();
                            testingLeaderElectionDriver.isLeader();
                            // leader contender is not granted leadership
                            assertThat(testingContender.getLeaderSessionID())
                                    .isEqualTo(nullValue());
                        });
            }
        };
    }

    @Test
    public void testOnLeaderInformationChangeIsIgnoredAfterBeingStop() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            testingLeaderElectionDriver.isLeader();
                            leaderElectionService.stop();
                            testingLeaderElectionDriver.leaderInformationChanged(
                                    LeaderInformation.empty());
                            // External storage should not be corrected
                            assertThat(testingLeaderElectionDriver.getLeaderInformation())
                                    .isEqualTo(LeaderInformation.empty());
                        });
            }
        };
    }

    @Test
    public void testOnRevokeLeadershipIsIgnoredAfterBeingStop() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            testingLeaderElectionDriver.isLeader();
                            final UUID oldSessionId = leaderElectionService.getLeaderSessionID();
                            assertThat(testingContender.getLeaderSessionID())
                                    .isEqualTo(oldSessionId);
                            leaderElectionService.stop();
                            testingLeaderElectionDriver.notLeader();
                            // leader contender is not revoked leadership
                            assertThat(testingContender.getLeaderSessionID())
                                    .isEqualTo(oldSessionId);
                        });
            }
        };
    }

    @Test
    public void testOldConfirmLeaderInformation() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            testingLeaderElectionDriver.isLeader();
                            final UUID currentLeaderSessionId =
                                    leaderElectionService.getLeaderSessionID();
                            assertThat(currentLeaderSessionId).isEqualTo(notNullValue());
                            // Old confirm call should be ignored.
                            leaderElectionService.confirmLeadership(UUID.randomUUID(), TEST_URL);
                            assertThat(leaderElectionService.getLeaderSessionID())
                                    .isEqualTo(currentLeaderSessionId);
                        });
            }
        };
    }

    @Test
    public void testErrorForwarding() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            final Exception testException = new Exception("test leader exception");
                            testingLeaderElectionDriver.onFatalError(testException);
                            testingContender.waitForError(timeout);
                            assertThat(testingContender.getError()).isEqualTo(notNullValue());
                            assertThat(testingContender.getError())
                                    .satisfies(
                                            matching(FlinkMatchers.containsCause(testException)));
                        });
            }
        };
    }

    @Test
    public void testErrorIsIgnoredAfterBeingStop() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            final Exception testException = new Exception("test leader exception");
                            leaderElectionService.stop();
                            testingLeaderElectionDriver.onFatalError(testException);
                            try {
                                testingContender.waitForError(timeout);
                                fail(
                                        "We expect to have a timeout here because there's no error should be passed to contender.");
                            } catch (TimeoutException ex) {
                                // noop
                            }
                            assertThat(testingContender.getError()).isEqualTo(nullValue());
                        });
            }
        };
    }

    /**
     * Tests that we can shut down the DefaultLeaderElectionService if the used LeaderElectionDriver
     * holds an internal lock. See FLINK-20008 for more details.
     */
    @Test
    public void testServiceShutDownWithSynchronizedDriver() throws Exception {
        final TestingLeaderElectionDriver.TestingLeaderElectionDriverFactory
                testingLeaderElectionDriverFactory =
                        new TestingLeaderElectionDriver.TestingLeaderElectionDriverFactory();
        final DefaultLeaderElectionService leaderElectionService =
                new DefaultLeaderElectionService(testingLeaderElectionDriverFactory);

        final TestingContender testingContender =
                new TestingContender(TEST_URL, leaderElectionService);

        leaderElectionService.start(testingContender);
        final TestingLeaderElectionDriver currentLeaderDriver =
                Preconditions.checkNotNull(
                        testingLeaderElectionDriverFactory.getCurrentLeaderDriver());

        final CheckedThread isLeaderThread =
                new CheckedThread() {
                    @Override
                    public void go() {
                        currentLeaderDriver.isLeader();
                    }
                };
        isLeaderThread.start();

        leaderElectionService.stop();
        isLeaderThread.sync();
    }

    private class Context {
        final TestingLeaderElectionDriver.TestingLeaderElectionDriverFactory
                testingLeaderElectionDriverFactory =
                        new TestingLeaderElectionDriver.TestingLeaderElectionDriverFactory();
        final DefaultLeaderElectionService leaderElectionService =
                new DefaultLeaderElectionService(testingLeaderElectionDriverFactory);
        final TestingContender testingContender =
                new TestingContender(TEST_URL, leaderElectionService);

        TestingLeaderElectionDriver testingLeaderElectionDriver;

        void runTest(RunnableWithException testMethod) throws Exception {
            leaderElectionService.start(testingContender);

            testingLeaderElectionDriver =
                    testingLeaderElectionDriverFactory.getCurrentLeaderDriver();
            assertThat(testingLeaderElectionDriver).isEqualTo(notNullValue());
            testMethod.run();

            leaderElectionService.stop();
        }
    }
}
