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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeoutTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ProcessingTimeoutTrigger}. */
public class ProcessingTimeoutTriggerTest {

    @Test
    public void testWindowFireWithoutResetTimer() throws Exception {
        TriggerTestHarness<Object, TimeWindow> testHarness =
                new TriggerTestHarness<>(
                        ProcessingTimeoutTrigger.of(
                                CountTrigger.of(3), Duration.ofMillis(50), false, true),
                        new TimeWindow.Serializer());

        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.CONTINUE);
        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.CONTINUE);

        // Should be two states, one for ProcessingTimeoutTrigger and one for CountTrigger.
        assertThat(testHarness.numStateEntries()).isEqualTo(2);
        assertThat(testHarness.numProcessingTimeTimers()).isEqualTo(1);
        assertThat(testHarness.numEventTimeTimers()).isEqualTo(0);

        // Should not fire before interval time.
        assertThatThrownBy(
                        () ->
                                testHarness.advanceProcessingTime(
                                        Long.MIN_VALUE + 40, new TimeWindow(0, 2)))
                .as("Must have exactly one timer firing. Fired timers: []")
                .isInstanceOf(IllegalStateException.class);

        assertThat(testHarness.advanceProcessingTime(Long.MIN_VALUE + 50, new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.FIRE);
        // After firing states should be clear.
        assertThat(testHarness.numStateEntries()).isEqualTo(0);
        assertThat(testHarness.numProcessingTimeTimers()).isEqualTo(0);
        assertThat(testHarness.numEventTimeTimers()).isEqualTo(0);

        // Check inner trigger is working as well
        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.CONTINUE);
        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.CONTINUE);
        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.FIRE);
    }

    @Test
    public void testWindowFireWithResetTimer() throws Exception {
        TriggerTestHarness<Object, TimeWindow> testHarness =
                new TriggerTestHarness<>(
                        ProcessingTimeoutTrigger.of(
                                CountTrigger.of(3), Duration.ofMillis(50), true, true),
                        new TimeWindow.Serializer());

        assertThatThrownBy(() -> testHarness.advanceProcessingTime(0, new TimeWindow(0, 2)))
                .as("Must have exactly one timer firing. Fired timers: []")
                .isInstanceOf(IllegalStateException.class);
        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.CONTINUE);
        assertThatThrownBy(() -> testHarness.advanceProcessingTime(10, new TimeWindow(0, 2)))
                .as("Must have exactly one timer firing. Fired timers: []")
                .isInstanceOf(IllegalStateException.class);
        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.CONTINUE);

        // Should be two states, one for ProcessingTimeoutTrigger and one for CountTrigger.
        assertThat(testHarness.numStateEntries()).isEqualTo(2);
        assertThat(testHarness.numProcessingTimeTimers()).isEqualTo(1);
        assertThat(testHarness.numEventTimeTimers()).isEqualTo(0);

        // Should not fire at timestampA+interval (at 50 millis), because resetTimer is on, it
        // should fire at 60 millis.
        assertThatThrownBy(() -> testHarness.advanceProcessingTime(50, new TimeWindow(0, 2)))
                .as("Must have exactly one timer firing. Fired timers: []")
                .isInstanceOf(IllegalStateException.class);

        assertThat(testHarness.advanceProcessingTime(60, new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.FIRE);
        // After firing states should be clear.
        assertThat(testHarness.numStateEntries()).isEqualTo(0);
        assertThat(testHarness.numProcessingTimeTimers()).isEqualTo(0);
        assertThat(testHarness.numEventTimeTimers()).isEqualTo(0);

        // Check inner trigger is working as well
        assertThat(testHarness.processElement(new StreamRecord<>(1, 0), new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.CONTINUE);
        assertThat(testHarness.processElement(new StreamRecord<>(1, 10), new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.CONTINUE);
        assertThat(testHarness.processElement(new StreamRecord<>(1, 20), new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.FIRE);
    }

    @Test
    public void testWindowFireWithoutClearOnTimeout() throws Exception {
        TriggerTestHarness<Object, TimeWindow> testHarness =
                new TriggerTestHarness<>(
                        ProcessingTimeoutTrigger.of(
                                CountTrigger.of(3), Duration.ofMillis(50), false, false),
                        new TimeWindow.Serializer());

        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.CONTINUE);
        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.CONTINUE);

        // Should be two states, one for ProcessingTimeoutTrigger and one for CountTrigger.
        assertThat(testHarness.numStateEntries()).isEqualTo(2);
        assertThat(testHarness.numProcessingTimeTimers()).isEqualTo(1);
        assertThat(testHarness.numEventTimeTimers()).isEqualTo(0);

        assertThat(testHarness.advanceProcessingTime(Long.MIN_VALUE + 50, new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.FIRE);

        // After firing, the state of the inner trigger (e.g CountTrigger) should not be clear, same
        // as the state of the timestamp.
        assertThat(testHarness.numStateEntries()).isEqualTo(2);
        assertThat(testHarness.numProcessingTimeTimers()).isEqualTo(0);
        assertThat(testHarness.numEventTimeTimers()).isEqualTo(0);
    }

    @Test
    public void testWindowPurgingWhenInnerTriggerIsPurging() throws Exception {
        TriggerTestHarness<Object, TimeWindow> testHarness =
                new TriggerTestHarness<>(
                        ProcessingTimeoutTrigger.of(
                                PurgingTrigger.of(ProcessingTimeTrigger.create()),
                                Duration.ofMillis(50),
                                false,
                                false),
                        new TimeWindow.Serializer());

        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.CONTINUE);
        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.CONTINUE);

        // Should be only one state for ProcessingTimeoutTrigger.
        assertThat(testHarness.numStateEntries()).isEqualTo(1);
        // Should be two timers, one for ProcessingTimeoutTrigger timeout timer, and one for
        // ProcessingTimeTrigger maxWindow timer.
        assertThat(testHarness.numProcessingTimeTimers()).isEqualTo(2);
        assertThat(testHarness.numEventTimeTimers()).isEqualTo(0);

        assertThat(testHarness.advanceProcessingTime(Long.MIN_VALUE + 50, new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.FIRE_AND_PURGE);

        // Because shouldClearAtTimeout is false, the state of ProcessingTimeoutTrigger not cleared,
        // same as ProcessingTimeTrigger timer for maxWindowTime.
        assertThat(testHarness.numStateEntries()).isEqualTo(1);
        assertThat(testHarness.numProcessingTimeTimers()).isEqualTo(1);
        assertThat(testHarness.numEventTimeTimers()).isEqualTo(0);
    }
}
