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

package org.apache.flink.table.persistedplan;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.persistedplan.infra.PersistedPlanTestCase;
import org.apache.flink.table.persistedplan.infra.RestoreTestExecutor;
import org.apache.flink.table.persistedplan.infra.SQLPipeline;
import org.apache.flink.test.util.SharedMiniClusterWithClientExtension;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.persistedplan.infra.PersistedPlanTestCase.SQLPipelineDefinition.DEFAULT_OUTPUT_TABLE_NAME;

class ProjectionAndCalcPersistedPlanTest {

    @RegisterExtension
    static final SharedMiniClusterWithClientExtension MINI_CLUSTER =
            new SharedMiniClusterWithClientExtension(
                    new MiniClusterResourceConfiguration.Builder().build());

    private static List<PersistedPlanTestCase> testCases =
            Stream.of(
                            SQLPipeline.builder()
                                    .savepointPhaseInput("MyInputTable", Row.of("a", "b", "c"))
                                    .setInsertSql(
                                            "INSERT INTO %s SELECT * FROM MyInputTable",
                                            DEFAULT_OUTPUT_TABLE_NAME)
                                    .savepointPhaseOutput(Row.of("a", "b", "c"))
                                    .executionPhaseInput("MyInputTable", Row.of("d", "e", "f"))
                                    .executionPhaseOutput(Row.of("d", "e", "f"))
                                    .build(),
                            new InsertIntoSelectAsteriskTestCase('f'),
                            new InsertIntoSelectAsteriskTestCase('h'))
                    .collect(Collectors.toList());

    @TestFactory
    Stream<DynamicTest> restoreTest() {
        return testCases.stream()
                .flatMap(
                        p ->
                                RestoreTestExecutor.forAllVersions(p)
                                        .withClusterClient(MINI_CLUSTER.getClusterClient())
                                        .build());
    }

    @TestFactory
    Stream<DynamicTest> anotherTest() {
        return testCases.stream()
                .flatMap(
                        p ->
                                AnotherTestExecutor.forAllVersions(p)
                                        .withClusterClient(MINI_CLUSTER.getClusterClient())
                                        .build());
    }
}
