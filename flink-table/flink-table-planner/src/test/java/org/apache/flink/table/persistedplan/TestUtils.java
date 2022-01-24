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

import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.ExceptionUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Comparator;

import static org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess.CHECKPOINT_DIR_PREFIX;
import static org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess.METADATA_FILE_NAME;

/** Test utilities. */
public class TestUtils {

    public static void submitJobAndWaitForResult(
            ClusterClient<?> client, JobGraph jobGraph, ClassLoader classLoader) throws Exception {
        client.submitJob(jobGraph)
                .thenCompose(client::requestJobResult)
                .get()
                .toJobExecutionResult(classLoader);
    }

    public static File getMostRecentCompletedCheckpoint(File checkpointDir) throws IOException {
        return Files.find(checkpointDir.toPath(), 2, TestUtils::isCompletedCheckpoint)
                .max(Comparator.comparing(Path::toString))
                .map(Path::toFile)
                .orElseThrow(() -> new IllegalStateException("Cannot generate checkpoint"));
    }

    private static boolean isCompletedCheckpoint(Path path, BasicFileAttributes attr) {
        return attr.isDirectory()
                && path.getFileName().toString().startsWith(CHECKPOINT_DIR_PREFIX)
                && hasMetadata(path);
    }

    private static boolean hasMetadata(Path file) {
        try {
            return Files.find(
                            file.toAbsolutePath(),
                            1,
                            (path, attrs) ->
                                    path.getFileName().toString().equals(METADATA_FILE_NAME))
                    .findAny()
                    .isPresent();
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
            return false; // should never happen
        }
    }
}
