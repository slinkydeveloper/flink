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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Optional;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link DefaultVertexParallelismInfo}. */
public class DefaultVertexParallelismInfoTest extends TestLogger {
    private static final Function<Integer, Optional<String>> ALWAYS_VALID =
            (max) -> Optional.empty();

    @Test
    public void parallelismInvalid() {
        assertThatThrownBy(() -> new DefaultVertexParallelismInfo(-1, 1, ALWAYS_VALID))
                .as("parallelism is not in valid bounds")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void maxParallelismInvalid() {
        assertThatThrownBy(() -> new DefaultVertexParallelismInfo(1, -1, ALWAYS_VALID))
                .as("max parallelism is not in valid bounds")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void setAutoMax() {
        DefaultVertexParallelismInfo info =
                new DefaultVertexParallelismInfo(
                        1, ExecutionConfig.PARALLELISM_AUTO_MAX, ALWAYS_VALID);

        assertThat(info.getMaxParallelism())
                .isEqualTo(KeyGroupRangeAssignment.UPPER_BOUND_MAX_PARALLELISM);
    }

    @Test
    public void canRescaleMaxOutOfBounds() {
        DefaultVertexParallelismInfo info = new DefaultVertexParallelismInfo(1, 1, ALWAYS_VALID);

        assertThatThrownBy(() -> info.canRescaleMaxParallelism(-4))
                .as("not in valid bounds")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void canRescaleMaxAuto() {
        DefaultVertexParallelismInfo info = new DefaultVertexParallelismInfo(1, 1, ALWAYS_VALID);

        assertThat(info.canRescaleMaxParallelism(ExecutionConfig.PARALLELISM_AUTO_MAX)).isTrue();
    }

    @Test
    public void canRescaleMax() {
        DefaultVertexParallelismInfo info = new DefaultVertexParallelismInfo(1, 1, ALWAYS_VALID);

        assertThat(info.canRescaleMaxParallelism(3)).isTrue();
    }

    @Test
    public void canRescaleMaxDefault() {
        DefaultVertexParallelismInfo info = new DefaultVertexParallelismInfo(1, 1, ALWAYS_VALID);

        assertThat(info.canRescaleMaxParallelism(JobVertex.MAX_PARALLELISM_DEFAULT)).isFalse();
    }

    @Test
    public void setMaxOutOfBounds() {
        DefaultVertexParallelismInfo info = new DefaultVertexParallelismInfo(1, 1, ALWAYS_VALID);

        assertThatThrownBy(
                        () -> {
                            info.setMaxParallelism(-4);
                        })
                .as("not in valid bounds")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void setMaxInvalid() {
        DefaultVertexParallelismInfo info =
                new DefaultVertexParallelismInfo(1, 1, (max) -> Optional.of("not valid"));

        assertThatThrownBy(
                        () -> {
                            info.setMaxParallelism(4);
                        })
                .as("not valid")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void setMaxValid() {
        DefaultVertexParallelismInfo info = new DefaultVertexParallelismInfo(1, 1, ALWAYS_VALID);

        info.setMaxParallelism(40);

        assertThat(info.getMaxParallelism()).isEqualTo(40);
    }
}
