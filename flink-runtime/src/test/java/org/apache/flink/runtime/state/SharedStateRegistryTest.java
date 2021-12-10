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

package org.apache.flink.runtime.state;

import org.apache.flink.core.fs.FSDataInputStream;

import org.junit.Test;

import java.io.IOException;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class SharedStateRegistryTest {

    /** Validate that all states can be correctly registered at the registry. */
    @Test
    public void testRegistryNormal() {

        SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

        // register one state
        TestSharedState firstState = new TestSharedState("first");
        SharedStateRegistry.Result result =
                sharedStateRegistry.registerReference(firstState.getRegistrationKey(), firstState);
        assertThat(result.getReferenceCount()).isEqualTo(1);
        assertThat(firstState == result.getReference()).isTrue();
        assertThat(firstState.isDiscarded()).isFalse();

        // register another state
        TestSharedState secondState = new TestSharedState("second");
        result =
                sharedStateRegistry.registerReference(
                        secondState.getRegistrationKey(), secondState);
        assertThat(result.getReferenceCount()).isEqualTo(1);
        assertThat(secondState == result.getReference()).isTrue();
        assertThat(firstState.isDiscarded()).isFalse();
        assertThat(secondState.isDiscarded()).isFalse();

        // attempt to register state under an existing key
        TestSharedState firstStatePrime =
                new TestSharedState(firstState.getRegistrationKey().getKeyString());
        result =
                sharedStateRegistry.registerReference(
                        firstState.getRegistrationKey(), firstStatePrime);
        assertThat(result.getReferenceCount()).isEqualTo(2);
        assertThat(firstStatePrime == result.getReference()).isFalse();
        assertThat(firstState == result.getReference()).isTrue();
        assertThat(firstStatePrime.isDiscarded()).isTrue();
        assertThat(firstState.isDiscarded()).isFalse();

        // reference the first state again
        result = sharedStateRegistry.registerReference(firstState.getRegistrationKey(), firstState);
        assertThat(result.getReferenceCount()).isEqualTo(3);
        assertThat(firstState == result.getReference()).isTrue();
        assertThat(firstState.isDiscarded()).isFalse();

        // unregister the second state
        result = sharedStateRegistry.unregisterReference(secondState.getRegistrationKey());
        assertThat(result.getReferenceCount()).isEqualTo(0);
        assertThat(result.getReference() == null).isTrue();
        assertThat(secondState.isDiscarded()).isTrue();

        // unregister the first state
        result = sharedStateRegistry.unregisterReference(firstState.getRegistrationKey());
        assertThat(result.getReferenceCount()).isEqualTo(2);
        assertThat(firstState == result.getReference()).isTrue();
        assertThat(firstState.isDiscarded()).isFalse();
    }

    /** Validate that unregister a nonexistent key will throw exception */
    @Test(expected = IllegalStateException.class)
    public void testUnregisterWithUnexistedKey() {
        SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
        sharedStateRegistry.unregisterReference(new SharedStateRegistryKey("non-existent"));
    }

    private static class TestSharedState implements StreamStateHandle {
        private static final long serialVersionUID = 4468635881465159780L;

        private SharedStateRegistryKey key;

        private boolean discarded;

        TestSharedState(String key) {
            this.key = new SharedStateRegistryKey(key);
            this.discarded = false;
        }

        public SharedStateRegistryKey getRegistrationKey() {
            return key;
        }

        @Override
        public void discardState() throws Exception {
            this.discarded = true;
        }

        @Override
        public long getStateSize() {
            return key.toString().length();
        }

        @Override
        public int hashCode() {
            return key.hashCode();
        }

        @Override
        public FSDataInputStream openInputStream() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<byte[]> asBytesIfInMemory() {
            return Optional.empty();
        }

        public boolean isDiscarded() {
            return discarded;
        }
    }
}
