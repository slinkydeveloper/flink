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

package org.apache.flink.graph.utils.proxy;

import org.apache.flink.graph.utils.proxy.OptionalBoolean.State;

import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OptionalBoolean}. */
public class OptionalBooleanTest {

    private OptionalBoolean u;
    private OptionalBoolean f;
    private OptionalBoolean t;
    private OptionalBoolean c;

    @Before
    public void setup() {
        u = new OptionalBoolean(false, true);
        f = new OptionalBoolean(false, true);
        t = new OptionalBoolean(false, true);
        c = new OptionalBoolean(false, true);

        f.set(false);
        t.set(true);

        c.set(true);
        c.mergeWith(f);
    }

    @Test
    public void testIsMismatchedWith() throws Exception {
        // unset, unset
        assertThat(u.conflictsWith(u)).isFalse();

        // unset, false
        assertThat(u.conflictsWith(f)).isFalse();

        // unset, true
        assertThat(u.conflictsWith(t)).isFalse();

        // unset, conflicting
        assertThat(u.conflictsWith(c)).isTrue();

        // false, unset
        assertThat(f.conflictsWith(u)).isFalse();

        // false, false
        assertThat(f.conflictsWith(f)).isFalse();

        // false, true
        assertThat(f.conflictsWith(t)).isTrue();

        // false, conflicting
        assertThat(f.conflictsWith(c)).isTrue();

        // true, unset
        assertThat(t.conflictsWith(u)).isFalse();

        // true, false
        assertThat(t.conflictsWith(f)).isTrue();

        // true, true
        assertThat(t.conflictsWith(t)).isFalse();

        // true, conflicting
        assertThat(t.conflictsWith(c)).isTrue();

        // conflicting, unset
        assertThat(c.conflictsWith(u)).isTrue();

        // conflicting, false
        assertThat(c.conflictsWith(f)).isTrue();

        // conflicting, true
        assertThat(c.conflictsWith(t)).isTrue();

        // conflicting, conflicting
        assertThat(c.conflictsWith(c)).isTrue();
    }

    @Test
    public void testMergeWith() throws Exception {
        // unset, unset => unset
        u.mergeWith(u);
        assertThat(u.getState()).isEqualTo(State.UNSET);

        // unset, false => false
        u.mergeWith(f);
        assertThat(u.getState()).isEqualTo(State.FALSE);
        u.unset();

        // unset, true => true
        u.mergeWith(t);
        assertThat(u.getState()).isEqualTo(State.TRUE);
        u.unset();

        // unset, conflicting => conflicting
        u.mergeWith(c);
        assertThat(u.getState()).isEqualTo(State.CONFLICTING);
        u.unset();

        // false, unset => false
        f.mergeWith(u);
        assertThat(f.getState()).isEqualTo(State.FALSE);

        // false, false => false
        f.mergeWith(f);
        assertThat(f.getState()).isEqualTo(State.FALSE);

        // false, true => conflicting
        f.mergeWith(t);
        assertThat(f.getState()).isEqualTo(State.CONFLICTING);
        f.set(false);

        // false, conflicting => conflicting
        f.mergeWith(c);
        assertThat(f.getState()).isEqualTo(State.CONFLICTING);
        f.set(false);

        // true, unset => true
        t.mergeWith(u);
        assertThat(t.getState()).isEqualTo(State.TRUE);

        // true, false => conflicting
        t.mergeWith(f);
        assertThat(t.getState()).isEqualTo(State.CONFLICTING);
        t.set(true);

        // true, true => true
        t.mergeWith(t);
        assertThat(t.getState()).isEqualTo(State.TRUE);

        // true, conflicting => conflicting
        t.mergeWith(c);
        assertThat(t.getState()).isEqualTo(State.CONFLICTING);
        t.set(true);

        // conflicting, unset => conflicting
        c.mergeWith(u);
        assertThat(c.getState()).isEqualTo(State.CONFLICTING);

        // conflicting, false => conflicting
        c.mergeWith(f);
        assertThat(c.getState()).isEqualTo(State.CONFLICTING);

        // conflicting, true => conflicting
        c.mergeWith(t);
        assertThat(c.getState()).isEqualTo(State.CONFLICTING);

        // conflicting, conflicting => conflicting
        c.mergeWith(c);
        assertThat(c.getState()).isEqualTo(State.CONFLICTING);
    }
}
