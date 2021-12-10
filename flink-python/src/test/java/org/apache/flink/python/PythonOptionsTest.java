/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.python;

import org.apache.flink.configuration.Configuration;

import org.junit.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;

/** Test all configurations can be set using configuration. */
public class PythonOptionsTest {

    @Test
    public void testBundleSize() {
        final Configuration configuration = new Configuration();
        final int defaultBundleSize = configuration.getInteger(PythonOptions.MAX_BUNDLE_SIZE);
        assertThat(defaultBundleSize)
                .isEqualTo(equalTo(PythonOptions.MAX_BUNDLE_SIZE.defaultValue()));

        final int expectedBundleSize = 100;
        configuration.setInteger(PythonOptions.MAX_BUNDLE_SIZE, expectedBundleSize);

        final int actualBundleSize = configuration.getInteger(PythonOptions.MAX_BUNDLE_SIZE);
        assertThat(actualBundleSize).isEqualTo(equalTo(expectedBundleSize));
    }

    @Test
    public void testBundleTime() {
        final Configuration configuration = new Configuration();
        final long defaultBundleTime = configuration.getLong(PythonOptions.MAX_BUNDLE_TIME_MILLS);
        assertThat(defaultBundleTime)
                .isEqualTo(equalTo(PythonOptions.MAX_BUNDLE_TIME_MILLS.defaultValue()));

        final long expectedBundleTime = 100;
        configuration.setLong(PythonOptions.MAX_BUNDLE_TIME_MILLS, expectedBundleTime);

        final long actualBundleSize = configuration.getLong(PythonOptions.MAX_BUNDLE_TIME_MILLS);
        assertThat(actualBundleSize).isEqualTo(equalTo(expectedBundleTime));
    }

    @Test
    public void testArrowBatchSize() {
        final Configuration configuration = new Configuration();
        final int defaultArrowBatchSize =
                configuration.getInteger(PythonOptions.MAX_ARROW_BATCH_SIZE);
        assertThat(defaultArrowBatchSize)
                .isEqualTo(equalTo(PythonOptions.MAX_ARROW_BATCH_SIZE.defaultValue()));

        final int expectedArrowBatchSize = 100;
        configuration.setInteger(PythonOptions.MAX_ARROW_BATCH_SIZE, expectedArrowBatchSize);

        final int actualArrowBatchSize =
                configuration.getInteger(PythonOptions.MAX_ARROW_BATCH_SIZE);
        assertThat(actualArrowBatchSize).isEqualTo(equalTo(expectedArrowBatchSize));
    }

    @Test
    public void testPythonMetricEnabled() {
        final Configuration configuration = new Configuration();
        final boolean isMetricEnabled =
                configuration.getBoolean(PythonOptions.PYTHON_METRIC_ENABLED);
        assertThat(isMetricEnabled)
                .isEqualTo(equalTo(PythonOptions.PYTHON_METRIC_ENABLED.defaultValue()));

        final boolean expectedIsMetricEnabled = false;
        configuration.setBoolean(PythonOptions.PYTHON_METRIC_ENABLED, false);

        final boolean actualIsMetricEnabled =
                configuration.getBoolean(PythonOptions.PYTHON_METRIC_ENABLED);
        assertThat(actualIsMetricEnabled).isEqualTo(equalTo(expectedIsMetricEnabled));
    }

    @Test
    public void testPythonProfileEnabled() {
        final Configuration configuration = new Configuration();
        final boolean isProfileEnabled =
                configuration.getBoolean(PythonOptions.PYTHON_PROFILE_ENABLED);
        assertThat(isProfileEnabled)
                .isEqualTo(equalTo(PythonOptions.PYTHON_PROFILE_ENABLED.defaultValue()));

        final boolean expectedIsProfileEnabled = true;
        configuration.setBoolean(PythonOptions.PYTHON_PROFILE_ENABLED, true);

        final boolean actualIsProfileEnabled =
                configuration.getBoolean(PythonOptions.PYTHON_PROFILE_ENABLED);
        assertThat(actualIsProfileEnabled).isEqualTo(equalTo(expectedIsProfileEnabled));
    }

    @Test
    public void testPythonFiles() {
        final Configuration configuration = new Configuration();
        final Optional<String> defaultPythonFiles =
                configuration.getOptional(PythonOptions.PYTHON_FILES);
        assertThat(defaultPythonFiles).isEqualTo(equalTo(Optional.empty()));

        final String expectedPythonFiles = "tmp_dir/test1.py,tmp_dir/test2.py";
        configuration.set(PythonOptions.PYTHON_FILES, expectedPythonFiles);

        final String actualPythonFiles = configuration.get(PythonOptions.PYTHON_FILES);
        assertThat(actualPythonFiles).isEqualTo(equalTo(expectedPythonFiles));
    }

    @Test
    public void testPythonRequirements() {
        final Configuration configuration = new Configuration();
        final Optional<String> defaultPythonRequirements =
                configuration.getOptional(PythonOptions.PYTHON_REQUIREMENTS);
        assertThat(defaultPythonRequirements).isEqualTo(equalTo(Optional.empty()));

        final String expectedPythonRequirements = "tmp_dir/requirements.txt#tmp_dir/cache";
        configuration.set(PythonOptions.PYTHON_REQUIREMENTS, expectedPythonRequirements);

        final String actualPythonRequirements =
                configuration.get(PythonOptions.PYTHON_REQUIREMENTS);
        assertThat(actualPythonRequirements).isEqualTo(equalTo(expectedPythonRequirements));
    }

    @Test
    public void testPythonArchives() {
        final Configuration configuration = new Configuration();
        final Optional<String> defaultPythonArchives =
                configuration.getOptional(PythonOptions.PYTHON_ARCHIVES);
        assertThat(defaultPythonArchives).isEqualTo(equalTo(Optional.empty()));

        final String expectedPythonArchives = "tmp_dir/py37.zip#venv,tmp_dir/data.zip";
        configuration.set(PythonOptions.PYTHON_ARCHIVES, expectedPythonArchives);

        final String actualPythonArchives = configuration.get(PythonOptions.PYTHON_ARCHIVES);
        assertThat(actualPythonArchives).isEqualTo(equalTo(expectedPythonArchives));
    }

    @Test
    public void testPythonExecutable() {
        final Configuration configuration = new Configuration();
        final Optional<String> defaultPythonExecutable =
                configuration.getOptional(PythonOptions.PYTHON_EXECUTABLE);
        assertThat(defaultPythonExecutable).isEqualTo(equalTo(Optional.empty()));

        final String expectedPythonExecutable = "venv/py37/bin/python";
        configuration.set(PythonOptions.PYTHON_EXECUTABLE, expectedPythonExecutable);

        final String actualPythonExecutable = configuration.get(PythonOptions.PYTHON_EXECUTABLE);
        assertThat(actualPythonExecutable).isEqualTo(equalTo(expectedPythonExecutable));
    }

    @Test
    public void testPythonClientExecutable() {
        final Configuration configuration = new Configuration();
        final Optional<String> defaultPythonClientExecutable =
                configuration.getOptional(PythonOptions.PYTHON_CLIENT_EXECUTABLE);
        assertThat(defaultPythonClientExecutable).isEqualTo(equalTo(Optional.empty()));

        final String expectedPythonClientExecutable = "tmp_dir/test1.py,tmp_dir/test2.py";
        configuration.set(PythonOptions.PYTHON_CLIENT_EXECUTABLE, expectedPythonClientExecutable);

        final String actualPythonClientExecutable =
                configuration.get(PythonOptions.PYTHON_CLIENT_EXECUTABLE);
        assertThat(actualPythonClientExecutable).isEqualTo(equalTo(expectedPythonClientExecutable));
    }
}
