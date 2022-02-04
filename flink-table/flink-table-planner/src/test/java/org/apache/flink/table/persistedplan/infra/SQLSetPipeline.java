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
import org.apache.flink.util.Preconditions;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static java.util.Collections.unmodifiableList;

/**
 * Simple static implementation of {@link SQLSetPipelineDefinition}. See {@link
 * PersistedPlanTestCase} for more details.
 */
final class SQLSetPipeline implements SQLSetPipelineDefinition {

    private final String name;
    private final String path;
    private final List<PipelineSource> savepointPhaseInput;
    private final List<String> pipeline;
    private final List<PipelineSink> savepointPhaseOutput;
    private final List<PipelineSource> executionPhaseInput;
    private final List<PipelineSink> executionPhaseOutput;

    private SQLSetPipeline(
            String name,
            String path,
            List<PipelineSource> savepointPhaseInput,
            List<String> pipeline,
            List<PipelineSink> savepointPhaseOutput,
            List<PipelineSource> executionPhaseInput,
            List<PipelineSink> executionPhaseOutput) {
        this.name = name;
        this.path = path;
        this.savepointPhaseInput = unmodifiableList(savepointPhaseInput);
        this.pipeline = unmodifiableList(pipeline);
        this.savepointPhaseOutput = unmodifiableList(savepointPhaseOutput);
        this.executionPhaseInput = unmodifiableList(executionPhaseInput);
        this.executionPhaseOutput = unmodifiableList(executionPhaseOutput);
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
    public List<String> pipeline(Context context) {
        return pipeline;
    }

    @Override
    public List<PipelineSink> savepointPhaseSinks(Context context) {
        return savepointPhaseOutput;
    }

    @Override
    public List<PipelineSource> sources(Context context) {
        return executionPhaseInput;
    }

    @Override
    public List<PipelineSink> sinks(Context context) {
        return executionPhaseOutput;
    }

    public static Builder named(String name) {
        String callerClassName = new Exception().getStackTrace()[1].getClassName();
        String packageName = callerClassName.substring(0, callerClassName.lastIndexOf("."));
        String path =
                File.pathSeparator
                        + packageName.replaceAll(Pattern.quote("."), File.pathSeparator)
                        + File.pathSeparator
                        + PersistedPlanTestCaseUtils.normalizeTestCaseName(name);
        return new Builder(name, path);
    }

    public static class Builder {

        private final String name;
        private final String path;

        private final List<PipelineSource> savepointPhaseInput = new ArrayList<>();
        private final List<String> pipeline = new ArrayList<>();
        private final List<PipelineSink> savepointPhaseOutput = new ArrayList<>();
        private final List<PipelineSource> executionPhaseInput = new ArrayList<>();
        private final List<PipelineSink> executionPhaseOutput = new ArrayList<>();

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
            this.pipeline.add(String.format(stmt, (Object[]) args));
            return this;
        }

        public Builder savepointPhaseOutput(List<PipelineSink> output) {
            savepointPhaseOutput.addAll(output);
            return this;
        }

        public Builder savepointPhaseOutput(PipelineSink... output) {
            savepointPhaseOutput.addAll(Arrays.asList(output));
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

        public Builder executionPhaseOutput(List<PipelineSink> output) {
            executionPhaseOutput.addAll(output);
            return this;
        }

        public Builder executionPhaseOutput(PipelineSink... output) {
            executionPhaseOutput.addAll(Arrays.asList(output));
            return this;
        }

        public SQLSetPipelineDefinition build() {
            Preconditions.checkArgument(
                    pipeline.size() != 0,
                    "Pipeline is empty. Define at least one insert statement");
            return new SQLSetPipeline(
                    name,
                    path,
                    savepointPhaseInput,
                    pipeline,
                    savepointPhaseOutput,
                    executionPhaseInput,
                    executionPhaseOutput);
        }
    }
}
