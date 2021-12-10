/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.checkpointing;

import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.source.TimestampedFileInputSplit;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Test the {@link TimestampedFileInputSplit} for Continuous File Processing. */
public class TimestampedFileInputSplitTest extends TestLogger {

    @Test
    public void testSplitEquality() {

        TimestampedFileInputSplit richFirstSplit =
                new TimestampedFileInputSplit(10, 2, new Path("test"), 0, 100, null);

        TimestampedFileInputSplit richSecondSplit =
                new TimestampedFileInputSplit(10, 2, new Path("test"), 0, 100, null);
        assertThat(richSecondSplit).isEqualTo(richFirstSplit);

        TimestampedFileInputSplit richModSecondSplit =
                new TimestampedFileInputSplit(11, 2, new Path("test"), 0, 100, null);
        assertThat(richModSecondSplit).isEqualTo(richSecondSplit);

        TimestampedFileInputSplit richThirdSplit =
                new TimestampedFileInputSplit(10, 2, new Path("test/test1"), 0, 100, null);
        assertThat(10).isEqualTo(richThirdSplit.getModificationTime());
        assertThat(richThirdSplit).isEqualTo(richFirstSplit);

        TimestampedFileInputSplit richThirdSplitCopy =
                new TimestampedFileInputSplit(10, 2, new Path("test/test1"), 0, 100, null);
        assertThat(richThirdSplit).isEqualTo(richThirdSplitCopy);
    }

    @Test
    public void testSplitComparison() {
        TimestampedFileInputSplit richFirstSplit =
                new TimestampedFileInputSplit(0, 3, new Path("test/test1"), 0, 100, null);

        TimestampedFileInputSplit richSecondSplit =
                new TimestampedFileInputSplit(10, 2, new Path("test/test2"), 0, 100, null);

        TimestampedFileInputSplit richThirdSplit =
                new TimestampedFileInputSplit(10, 1, new Path("test/test2"), 0, 100, null);

        TimestampedFileInputSplit richForthSplit =
                new TimestampedFileInputSplit(11, 0, new Path("test/test3"), 0, 100, null);

        TimestampedFileInputSplit richFifthSplit =
                new TimestampedFileInputSplit(11, 1, new Path("test/test3"), 0, 100, null);

        // smaller mod time
        assertThat(richFirstSplit.compareTo(richSecondSplit) < 0).isTrue();

        // lexicographically on the path
        assertThat(richThirdSplit.compareTo(richFifthSplit) < 0).isTrue();

        // same mod time, same file so smaller split number first
        assertThat(richThirdSplit.compareTo(richSecondSplit) < 0).isTrue();

        // smaller modification time first
        assertThat(richThirdSplit.compareTo(richForthSplit) < 0).isTrue();
    }

    @Test
    public void testIllegalArgument() {
        try {
            new TimestampedFileInputSplit(
                    -10, 2, new Path("test"), 0, 100, null); // invalid modification time
        } catch (Exception e) {
            if (!(e instanceof IllegalArgumentException)) {
                fail(e.getMessage());
            }
        }
    }

    @Test
    public void testPriorityQ() {
        TimestampedFileInputSplit richFirstSplit =
                new TimestampedFileInputSplit(0, 3, new Path("test/test1"), 0, 100, null);

        TimestampedFileInputSplit richSecondSplit =
                new TimestampedFileInputSplit(10, 2, new Path("test/test2"), 0, 100, null);

        TimestampedFileInputSplit richThirdSplit =
                new TimestampedFileInputSplit(10, 1, new Path("test/test2"), 0, 100, null);

        TimestampedFileInputSplit richForthSplit =
                new TimestampedFileInputSplit(11, 0, new Path("test/test3"), 0, 100, null);

        TimestampedFileInputSplit richFifthSplit =
                new TimestampedFileInputSplit(11, 1, new Path("test/test3"), 0, 100, null);

        Queue<TimestampedFileInputSplit> pendingSplits = new PriorityQueue<>();

        pendingSplits.add(richSecondSplit);
        pendingSplits.add(richForthSplit);
        pendingSplits.add(richFirstSplit);
        pendingSplits.add(richFifthSplit);
        pendingSplits.add(richFifthSplit);
        pendingSplits.add(richThirdSplit);

        List<TimestampedFileInputSplit> actualSortedSplits = new ArrayList<>();
        while (true) {
            actualSortedSplits.add(pendingSplits.poll());
            if (pendingSplits.isEmpty()) {
                break;
            }
        }

        List<TimestampedFileInputSplit> expectedSortedSplits = new ArrayList<>();
        expectedSortedSplits.add(richFirstSplit);
        expectedSortedSplits.add(richThirdSplit);
        expectedSortedSplits.add(richSecondSplit);
        expectedSortedSplits.add(richForthSplit);
        expectedSortedSplits.add(richFifthSplit);
        expectedSortedSplits.add(richFifthSplit);

        assertThat(actualSortedSplits.toArray()).isEqualTo(expectedSortedSplits.toArray());
    }
}
