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

import org.apache.flink.table.persistedplan.infra.PersistedPlanTestCase;
import org.apache.flink.table.test.pipeline.PipelineSink;
import org.apache.flink.table.test.pipeline.PipelineSource;
import org.apache.flink.table.test.pipeline.Pipelines;
import org.apache.flink.types.Row;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.apache.flink.table.test.pipeline.Pipelines.source;

public class InsertIntoSelectAsteriskTestCase
        implements PersistedPlanTestCase, PersistedPlanTestCase.SQLPipelineDefinition {

    private final char startingChar;

    public InsertIntoSelectAsteriskTestCase(char startingChar) {
        this.startingChar = startingChar;
    }

    @Override
    public String getName() {
        return "INSERT INTO SELECT * with input data starting from char " + startingChar;
    }

    @Override
    public List<PipelineSource> savepointPhaseSources(PersistedPlanTestCase.Context context) {
        return singletonList(
                source("A")
                        .rows(
                                Row.of(
                                        String.valueOf(startingChar),
                                        String.valueOf(startingChar + 1),
                                        String.valueOf(startingChar + 2))));
    }

    @Override
    public String pipeline(PersistedPlanTestCase.Context context) {
        return "INSERT INTO B SELECT * FROM A";
    }

    @Override
    public PipelineSink savepointPhaseSink(PersistedPlanTestCase.Context context) {
        return Pipelines.sink("B")
                .rows(
                        Row.of(
                                String.valueOf(startingChar),
                                String.valueOf(startingChar + 1),
                                String.valueOf(startingChar + 2)));
    }

    @Override
    public List<PipelineSource> sources(Context context) {
        return singletonList(
                source("A")
                        .rows(
                                Row.of(
                                        String.valueOf(startingChar + 3),
                                        String.valueOf(startingChar + 4),
                                        String.valueOf(startingChar + 5))));
    }

    @Override
    public PipelineSink sink(Context context) {
        return Pipelines.sink("B")
                .rows(
                        Row.of(
                                String.valueOf(startingChar + 3),
                                String.valueOf(startingChar + 4),
                                String.valueOf(startingChar + 5)));
    }
}
