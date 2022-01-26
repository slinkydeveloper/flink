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
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

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
    public Map<String, List<Row>> savepointPhaseSources(PersistedPlanTestCase.Context context) {
        return singletonMap(
                "MyInputTable",
                singletonList(
                        Row.of(
                                String.valueOf(startingChar),
                                String.valueOf(startingChar + 1),
                                String.valueOf(startingChar + 2))));
    }

    @Override
    public String pipeline(PersistedPlanTestCase.Context context) {
        return format("INSERT INTO %s SELECT * FROM MyInputTable", DEFAULT_OUTPUT_TABLE_NAME);
    }

    @Override
    public List<Row> savepointPhaseSink(PersistedPlanTestCase.Context context) {
        return singletonList(
                Row.of(
                        String.valueOf(startingChar),
                        String.valueOf(startingChar + 1),
                        String.valueOf(startingChar + 2)));
    }

    @Override
    public Map<String, List<Row>> sources(PersistedPlanTestCase.Context context) {
        return singletonMap(
                "MyInputTable",
                singletonList(
                        Row.of(
                                String.valueOf(startingChar + 3),
                                String.valueOf(startingChar + 4),
                                String.valueOf(startingChar + 5))));
    }

    @Override
    public List<Row> sink(PersistedPlanTestCase.Context context) {
        return singletonList(
                Row.of(
                        String.valueOf(startingChar + 3),
                        String.valueOf(startingChar + 4),
                        String.valueOf(startingChar + 5)));
    }
}
