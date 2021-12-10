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

package org.apache.flink.optimizer.plandump;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class NumberFormattingTest {

    @Test
    public void testFormatNumberNoDigit() {
        assertThat(PlanJSONDumpGenerator.formatNumber(0)).isEqualTo("0.0");
        assertThat(PlanJSONDumpGenerator.formatNumber(0.0000000001)).isEqualTo("0.00");
        assertThat(PlanJSONDumpGenerator.formatNumber(-1.0)).isEqualTo("-1.0");
        assertThat(PlanJSONDumpGenerator.formatNumber(1)).isEqualTo("1.00");
        assertThat(PlanJSONDumpGenerator.formatNumber(17)).isEqualTo("17.00");
        assertThat(PlanJSONDumpGenerator.formatNumber(17.44)).isEqualTo("17.44");
        assertThat(PlanJSONDumpGenerator.formatNumber(143)).isEqualTo("143.00");
        assertThat(PlanJSONDumpGenerator.formatNumber(143.4)).isEqualTo("143.40");
        assertThat(PlanJSONDumpGenerator.formatNumber(143.5)).isEqualTo("143.50");
        assertThat(PlanJSONDumpGenerator.formatNumber(143.6)).isEqualTo("143.60");
        assertThat(PlanJSONDumpGenerator.formatNumber(143.45)).isEqualTo("143.45");
        assertThat(PlanJSONDumpGenerator.formatNumber(143.55)).isEqualTo("143.55");
        assertThat(PlanJSONDumpGenerator.formatNumber(143.65)).isEqualTo("143.65");
        assertThat(PlanJSONDumpGenerator.formatNumber(143.655)).isEqualTo("143.66");

        assertThat(PlanJSONDumpGenerator.formatNumber(1126.0)).isEqualTo("1.13 K");
        assertThat(PlanJSONDumpGenerator.formatNumber(11126.0)).isEqualTo("11.13 K");
        assertThat(PlanJSONDumpGenerator.formatNumber(118126.0)).isEqualTo("118.13 K");

        assertThat(PlanJSONDumpGenerator.formatNumber(1435126.0)).isEqualTo("1.44 M");
    }
}
