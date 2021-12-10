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

package org.apache.flink.graph.bipartite;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BipartiteEdge}. */
public class BipartiteEdgeTest {

    private static final int BOTTOM_ID = 0;
    private static final int TOP_ID = 1;
    private static final String VALUE = "value";

    private final BipartiteEdge<Integer, Integer, String> edge = createEdge();

    @Test
    public void testGetBottomId() {
        assertThat((long) edge.getBottomId()).isEqualTo(BOTTOM_ID);
    }

    @Test
    public void testGetTopId() {
        assertThat((long) edge.getTopId()).isEqualTo(TOP_ID);
    }

    @Test
    public void testGetValue() {
        assertThat(edge.getValue()).isEqualTo(VALUE);
    }

    @Test
    public void testSetBottomId() {
        edge.setBottomId(100);
        assertThat((long) edge.getBottomId()).isEqualTo(100);
    }

    @Test
    public void testSetTopId() {
        edge.setTopId(100);
        assertThat((long) edge.getTopId()).isEqualTo(100);
    }

    @Test
    public void testSetValue() {
        edge.setValue("newVal");
        assertThat(edge.getValue()).isEqualTo("newVal");
    }

    private BipartiteEdge<Integer, Integer, String> createEdge() {
        return new BipartiteEdge<>(TOP_ID, BOTTOM_ID, VALUE);
    }
}
