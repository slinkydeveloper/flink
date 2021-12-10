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

package org.apache.flink.metrics.slf4j;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.util.TestHistogram;
import org.apache.flink.metrics.util.TestMetricGroup;
import org.apache.flink.testutils.logging.TestLoggerResource;
import org.apache.flink.util.TestLogger;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.event.Level;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link Slf4jReporter}. */
public class Slf4jReporterTest extends TestLogger {

    private static final String SCOPE = "scope";
    private static char delimiter;

    private static MetricGroup metricGroup;
    private static Slf4jReporter reporter;

    @Rule
    public final TestLoggerResource testLoggerResource =
            new TestLoggerResource(Slf4jReporter.class, Level.INFO);

    @BeforeClass
    public static void setUp() {
        delimiter = '.';

        metricGroup =
                TestMetricGroup.newBuilder()
                        .setMetricIdentifierFunction((s, characterFilter) -> SCOPE + delimiter + s)
                        .build();
        reporter = new Slf4jReporter();
        reporter.open(new MetricConfig());
    }

    @Test
    public void testAddCounter() throws Exception {
        String counterName = "simpleCounter";

        SimpleCounter counter = new SimpleCounter();
        reporter.notifyOfAddedMetric(counter, counterName, metricGroup);

        assertThat(reporter.getCounters().containsKey(counter)).isTrue();

        String expectedCounterReport =
                reporter.filterCharacters(SCOPE)
                        + delimiter
                        + reporter.filterCharacters(counterName)
                        + ": 0";

        reporter.report();
        assertThat(testLoggerResource.getMessages()).contains(expectedCounterReport);
    }

    @Test
    public void testAddGauge() throws Exception {
        String gaugeName = "gauge";

        Gauge<Long> gauge = () -> null;
        reporter.notifyOfAddedMetric(gauge, gaugeName, metricGroup);
        assertThat(reporter.getGauges().containsKey(gauge)).isTrue();

        String expectedGaugeReport =
                reporter.filterCharacters(SCOPE)
                        + delimiter
                        + reporter.filterCharacters(gaugeName)
                        + ": null";

        reporter.report();
        assertThat(testLoggerResource.getMessages()).contains(expectedGaugeReport);
    }

    @Test
    public void testAddMeter() throws Exception {
        String meterName = "meter";

        Meter meter = new MeterView(5);
        reporter.notifyOfAddedMetric(meter, meterName, metricGroup);
        assertThat(reporter.getMeters().containsKey(meter)).isTrue();

        String expectedMeterReport =
                reporter.filterCharacters(SCOPE)
                        + delimiter
                        + reporter.filterCharacters(meterName)
                        + ": 0.0";

        reporter.report();
        assertThat(testLoggerResource.getMessages()).contains(expectedMeterReport);
    }

    @Test
    public void testAddHistogram() throws Exception {
        String histogramName = "histogram";

        Histogram histogram = new TestHistogram();
        reporter.notifyOfAddedMetric(histogram, histogramName, metricGroup);
        assertThat(reporter.getHistograms().containsKey(histogram)).isTrue();

        String expectedHistogramName =
                reporter.filterCharacters(SCOPE)
                        + delimiter
                        + reporter.filterCharacters(histogramName);

        reporter.report();
        assertThat(testLoggerResource.getMessages()).contains(expectedHistogramName);
    }

    @Test
    public void testFilterCharacters() throws Exception {
        assertThat(reporter.filterCharacters("")).isEqualTo("");
        assertThat(reporter.filterCharacters("abc")).isEqualTo("abc");
        assertThat(reporter.filterCharacters("a:b$%^::")).isEqualTo("a:b$%^::");
    }
}
