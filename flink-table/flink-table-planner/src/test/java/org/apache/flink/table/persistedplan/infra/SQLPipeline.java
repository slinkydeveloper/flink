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

package org.apache.flink.table.persistedplan.infra;

import org.apache.flink.table.test.pipeline.PipelineSink;
import org.apache.flink.table.test.pipeline.PipelineSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Simple static implementation of {@link SQLPipelineDefinition}. See {@link PersistedPlanTestCase}
 * for more details.
 */
final class SQLPipeline implements SQLPipelineDefinition {

    private final String name;
    private final String path;
    private final List<PipelineSource> savepointPhaseInput;
    private final String pipeline;
    private final PipelineSink savepointPhaseOutput;
    private final List<PipelineSource> executionPhaseInput;
    private final PipelineSink executionPhaseOutput;

    private SQLPipeline(
            String name,
            String path,
            List<PipelineSource> savepointPhaseInput,
            String pipeline,
            PipelineSink savepointPhaseOutput,
            List<PipelineSource> executionPhaseInput,
            PipelineSink executionPhaseOutput) {
        this.name = name;
        this.path = path;
        this.savepointPhaseInput = unmodifiableList(savepointPhaseInput);
        this.pipeline = pipeline;
        this.savepointPhaseOutput = savepointPhaseOutput;
        this.executionPhaseInput = unmodifiableList(executionPhaseInput);
        this.executionPhaseOutput = executionPhaseOutput;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public List<PipelineSource> savepointPhaseSources(Context context) {
        return savepointPhaseInput;
    }

    @Override
    public String pipeline(Context context) {
        return pipeline;
    }

    @Override
    public PipelineSink savepointPhaseSink(Context context) {
        return savepointPhaseOutput;
    }

    @Override
    public List<PipelineSource> sources(Context context) {
        return executionPhaseInput;
    }

    @Override
    public PipelineSink sink(Context context) {
        return executionPhaseOutput;
    }

    public static class Builder {

        private final String name;
        private final String path;

        private final List<PipelineSource> savepointPhaseInput = new ArrayList<>();
        private String pipeline;
        private PipelineSink savepointPhaseOutput;
        private final List<PipelineSource> executionPhaseInput = new ArrayList<>();
        private PipelineSink executionPhaseOutput;

        public Builder(String name, String path) {
            this.name = name;
            this.path = path;
        }

        public Builder savepointPhaseInput(List<PipelineSource> input) {
            savepointPhaseInput.addAll(input);
            return this;
        }

        public Builder savepointPhaseInput(PipelineSource... input) {
            savepointPhaseInput.addAll(Arrays.asList(input));
            return this;
        }

        public Builder sql(String stmt, String... args) {
            this.pipeline = String.format(stmt, (Object[]) args);
            return this;
        }

        public Builder savepointPhaseOutput(PipelineSink output) {
            this.savepointPhaseOutput = output;
            return this;
        }

        public Builder executionPhaseInput(List<PipelineSource> input) {
            executionPhaseInput.addAll(input);
            return this;
        }

        public Builder executionPhaseInput(PipelineSource... input) {
            executionPhaseInput.addAll(Arrays.asList(input));
            return this;
        }

        public Builder executionPhaseOutput(PipelineSink output) {
            this.executionPhaseOutput = output;
            return this;
        }

        public SQLPipelineDefinition build() {
            return new SQLPipeline(
                    name,
                    path,
                    savepointPhaseInput,
                    checkNotNull(pipeline, "Pipeline is not defined."),
                    checkNotNull(savepointPhaseOutput, "Savepoint phase output not defined"),
                    executionPhaseInput,
                    checkNotNull(executionPhaseOutput, "Execution phase output not defined"));
        }
    }
}
