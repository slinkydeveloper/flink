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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TimeWindow}. */
public class TimeWindowTest {
    @Test
    public void testGetWindowStartWithOffset() {
        // [0, 7), [7, 14), [14, 21)...
        long offset = 0;
        assertThat(0).isEqualTo(TimeWindow.getWindowStartWithOffset(1, offset, 7));
        assertThat(0).isEqualTo(TimeWindow.getWindowStartWithOffset(6, offset, 7));
        assertThat(7).isEqualTo(TimeWindow.getWindowStartWithOffset(7, offset, 7));
        assertThat(7).isEqualTo(TimeWindow.getWindowStartWithOffset(8, offset, 7));

        // [-4, 3), [3, 10), [10, 17)...
        offset = 3;
        assertThat(-4).isEqualTo(TimeWindow.getWindowStartWithOffset(1, offset, 7));
        assertThat(-4).isEqualTo(TimeWindow.getWindowStartWithOffset(2, offset, 7));
        assertThat(3).isEqualTo(TimeWindow.getWindowStartWithOffset(3, offset, 7));
        assertThat(3).isEqualTo(TimeWindow.getWindowStartWithOffset(9, offset, 7));
        assertThat(10).isEqualTo(TimeWindow.getWindowStartWithOffset(10, offset, 7));

        // [-2, 5), [5, 12), [12, 19)...
        offset = -2;
        assertThat(-2).isEqualTo(TimeWindow.getWindowStartWithOffset(1, offset, 7));
        assertThat(-2).isEqualTo(TimeWindow.getWindowStartWithOffset(-2, offset, 7));
        assertThat(-2).isEqualTo(TimeWindow.getWindowStartWithOffset(3, offset, 7));
        assertThat(-2).isEqualTo(TimeWindow.getWindowStartWithOffset(4, offset, 7));
        assertThat(5).isEqualTo(TimeWindow.getWindowStartWithOffset(7, offset, 7));
        assertThat(12).isEqualTo(TimeWindow.getWindowStartWithOffset(12, offset, 7));

        // for GMT+08:00
        offset = -TimeUnit.HOURS.toMillis(8);
        long size = TimeUnit.DAYS.toMillis(1);
        assertThat(1470844800000L)
                .isEqualTo(TimeWindow.getWindowStartWithOffset(1470902048450L, offset, size));
    }

    private boolean intersects(TimeWindow w0, TimeWindow w1) {
        assertThat(w1.intersects(w0)).isEqualTo(w0.intersects(w1));
        return w0.intersects(w1);
    }

    @Test
    public void testIntersect() {
        // must intersect with itself
        TimeWindow window = new TimeWindow(10, 20);
        assertThat(window.intersects(window)).isTrue();

        // windows are next to each other
        assertThat(intersects(window, new TimeWindow(20, 30))).isTrue();

        // there is distance between the windows
        assertThat(intersects(window, new TimeWindow(21, 30))).isFalse();

        // overlaps by one
        assertThat(intersects(window, new TimeWindow(19, 22))).isTrue();
    }
}
