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

package org.apache.flink.runtime.throwable;

import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test {@link ThrowableClassifier}. */
public class ThrowableClassifierTest extends TestLogger {

    @Test
    public void testThrowableType_NonRecoverable() {
        assertThat(
                        ThrowableClassifier.getThrowableType(
                                new SuppressRestartsException(new Exception(""))))
                .isEqualTo(ThrowableType.NonRecoverableError);
    }

    @Test
    public void testThrowableType_Recoverable() {
        assertThat(ThrowableClassifier.getThrowableType(new Exception("")))
                .isEqualTo(ThrowableType.RecoverableError);
        assertThat(ThrowableClassifier.getThrowableType(new TestRecoverableErrorException()))
                .isEqualTo(ThrowableType.RecoverableError);
    }

    @Test
    public void testThrowableType_EnvironmentError() {
        assertThat(ThrowableClassifier.getThrowableType(new TestEnvironmentErrorException()))
                .isEqualTo(ThrowableType.EnvironmentError);
    }

    @Test
    public void testThrowableType_PartitionDataMissingError() {
        assertThat(
                        ThrowableClassifier.getThrowableType(
                                new TestPartitionDataMissingErrorException()))
                .isEqualTo(ThrowableType.PartitionDataMissingError);
    }

    @Test
    public void testThrowableType_InheritError() {
        assertThat(
                        ThrowableClassifier.getThrowableType(
                                new TestPartitionDataMissingErrorSubException()))
                .isEqualTo(ThrowableType.PartitionDataMissingError);
    }

    @Test
    public void testFindThrowableOfThrowableType() {
        // no throwable type
        assertThat(
                        ThrowableClassifier.findThrowableOfThrowableType(
                                        new Exception(), ThrowableType.RecoverableError)
                                .isPresent())
                .isFalse();

        // no recoverable throwable type
        assertThat(
                        ThrowableClassifier.findThrowableOfThrowableType(
                                        new TestPartitionDataMissingErrorException(),
                                        ThrowableType.RecoverableError)
                                .isPresent())
                .isFalse();

        // direct recoverable throwable
        assertThat(
                        ThrowableClassifier.findThrowableOfThrowableType(
                                        new TestRecoverableErrorException(),
                                        ThrowableType.RecoverableError)
                                .isPresent())
                .isTrue();

        // nested recoverable throwable
        assertThat(
                        ThrowableClassifier.findThrowableOfThrowableType(
                                        new Exception(new TestRecoverableErrorException()),
                                        ThrowableType.RecoverableError)
                                .isPresent())
                .isTrue();

        // inherit recoverable throwable
        assertThat(
                        ThrowableClassifier.findThrowableOfThrowableType(
                                        new TestRecoverableFailureSubException(),
                                        ThrowableType.RecoverableError)
                                .isPresent())
                .isTrue();
    }

    @ThrowableAnnotation(ThrowableType.PartitionDataMissingError)
    private class TestPartitionDataMissingErrorException extends Exception {}

    @ThrowableAnnotation(ThrowableType.EnvironmentError)
    private class TestEnvironmentErrorException extends Exception {}

    @ThrowableAnnotation(ThrowableType.RecoverableError)
    private class TestRecoverableErrorException extends Exception {}

    private class TestPartitionDataMissingErrorSubException
            extends TestPartitionDataMissingErrorException {}

    private class TestRecoverableFailureSubException extends TestRecoverableErrorException {}
}
