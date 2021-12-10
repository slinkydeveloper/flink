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

package org.apache.flink.graph.drivers.parameter;

import org.apache.flink.api.java.utils.ParameterTool;

import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/** Tests for {@link IterationConvergence}. */
public class IterationConvergenceTest extends ParameterTestBase {

    private IterationConvergence parameter;

    @Before
    public void setup() {
        super.setup();

        parameter = new IterationConvergence(owner, 10);
    }

    @Test
    public void testWithIterations() {
        parameter.configure(ParameterTool.fromArgs(new String[] {"--iterations", "42"}));
        assertThat(parameter.getValue().iterations).isEqualTo(42);
        assertThat(parameter.getValue().convergenceThreshold)
                .isCloseTo(Double.MAX_VALUE, within(0.000001));
    }

    @Test
    public void testWithConvergenceThreshold() {
        parameter.configure(ParameterTool.fromArgs(new String[] {"--convergence_threshold", "42"}));
        assertThat(parameter.getValue().iterations).isEqualTo(Integer.MAX_VALUE);
        assertThat(parameter.getValue().convergenceThreshold).isCloseTo(42.0, within(0.000001));
    }

    @Test
    public void testWithBoth() {
        parameter.configure(
                ParameterTool.fromArgs(
                        new String[] {"--iterations", "42", "--convergence_threshold", "42"}));
        assertThat(parameter.getValue().iterations).isEqualTo(42);
        assertThat(parameter.getValue().convergenceThreshold).isCloseTo(42.0, within(0.000001));
    }

    @Test
    public void testWithNeither() {
        parameter.configure(ParameterTool.fromArgs(new String[] {}));
        assertThat(parameter.getValue().iterations).isEqualTo(10);
        assertThat(parameter.getValue().convergenceThreshold)
                .isCloseTo(Double.MAX_VALUE, within(0.000001));
    }
}
