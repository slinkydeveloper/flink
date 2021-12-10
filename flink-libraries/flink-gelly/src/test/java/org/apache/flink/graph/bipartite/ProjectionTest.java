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

import org.apache.flink.graph.Vertex;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link Projection}. */
public class ProjectionTest {

    private static final int ID = 10;

    private static final String VERTEX_VALUE = "vertex-value";
    private static final String SOURCE_EDGE_VALUE = "source-edge-value";
    private static final String TARGET_EDGE_VALUE = "target-edge-value";
    private static final String SOURCE_VERTEX_VALUE = "source-vertex-value";
    private static final String TARGET_VERTEX_VALUE = "target-vertex-value";

    private Projection<Integer, String, String, String> projection = createProjection();

    @Test
    public void testIntermediateVertexGetId() {
        assertThat(projection.getIntermediateVertexId()).isEqualTo(Integer.valueOf(ID));
    }

    @Test
    public void testGetIntermediateVertexValue() {
        assertThat(projection.getIntermediateVertexValue()).isEqualTo(VERTEX_VALUE);
    }

    @Test
    public void testGetSourceEdgeValue() {
        assertThat(projection.getSourceEdgeValue()).isEqualTo(SOURCE_EDGE_VALUE);
    }

    @Test
    public void testGetTargetEdgeValue() {
        assertThat(projection.getTargetEdgeValue()).isEqualTo(TARGET_EDGE_VALUE);
    }

    @Test
    public void testGetSourceVertexValue() {
        assertThat(projection.getsSourceVertexValue()).isEqualTo(SOURCE_VERTEX_VALUE);
    }

    @Test
    public void testGetTargetVertexValue() {
        assertThat(projection.getTargetVertexValue()).isEqualTo(TARGET_VERTEX_VALUE);
    }

    private Projection<Integer, String, String, String> createProjection() {
        return new Projection<>(
                new Vertex<>(ID, VERTEX_VALUE),
                SOURCE_VERTEX_VALUE,
                TARGET_VERTEX_VALUE,
                SOURCE_EDGE_VALUE,
                TARGET_EDGE_VALUE);
    }
}
