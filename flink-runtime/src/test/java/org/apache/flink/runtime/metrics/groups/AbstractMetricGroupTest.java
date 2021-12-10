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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.core.testutils.BlockerSync;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.metrics.CollectingMetricsReporter;
import org.apache.flink.runtime.metrics.CollectingMetricsReporter.MetricGroupAndName;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.MetricRegistryTestUtils;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.ReporterSetup;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.runtime.metrics.util.TestingMetricRegistry;
import org.apache.flink.util.TestLogger;

import org.hamcrest.collection.IsMapContaining;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.HamcrestCondition.matching;
import static org.hamcrest.core.IsNot.not;

/** Tests for the {@link AbstractMetricGroup}. */
public class AbstractMetricGroupTest extends TestLogger {
    /**
     * Verifies that no {@link NullPointerException} is thrown when {@link
     * AbstractMetricGroup#getAllVariables()} is called and the parent is null.
     */
    @Test
    public void testGetAllVariables() throws Exception {
        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration());

        AbstractMetricGroup group =
                new AbstractMetricGroup<AbstractMetricGroup<?>>(registry, new String[0], null) {
                    @Override
                    protected QueryScopeInfo createQueryServiceMetricInfo(CharacterFilter filter) {
                        return null;
                    }

                    @Override
                    protected String getGroupName(CharacterFilter filter) {
                        return "";
                    }
                };
        assertThat(group.getAllVariables().isEmpty()).isTrue();

        registry.shutdown().get();
    }

    @Test
    public void testGetAllVariablesWithOutExclusions() {
        MetricRegistry registry = NoOpMetricRegistry.INSTANCE;

        AbstractMetricGroup<?> group = new ProcessMetricGroup(registry, "host");
        assertThat(group.getAllVariables())
                .satisfies(matching(IsMapContaining.hasKey(ScopeFormat.SCOPE_HOST)));
    }

    @Test
    public void testGetAllVariablesWithExclusions() {
        MetricRegistry registry = NoOpMetricRegistry.INSTANCE;

        AbstractMetricGroup<?> group = new ProcessMetricGroup(registry, "host");
        assertThat(0)
                .isEqualTo(
                        group.getAllVariables(-1, Collections.singleton(ScopeFormat.SCOPE_HOST))
                                .size());
    }

    @Test
    public void testGetAllVariablesWithExclusionsForReporters() {
        MetricRegistry registry = TestingMetricRegistry.builder().setNumberReporters(2).build();

        AbstractMetricGroup<?> group =
                new GenericMetricGroup(registry, null, "test") {
                    @Override
                    protected void putVariables(Map<String, String> variables) {
                        variables.put("k1", "v1");
                        variables.put("k2", "v2");
                    }
                };

        group.getAllVariables(-1, Collections.emptySet());

        assertThat(group.getAllVariables(0, Collections.singleton("k1")))
                .satisfies(matching(not(IsMapContaining.hasKey("k1"))));
        assertThat(group.getAllVariables(0, Collections.singleton("k1")))
                .satisfies(matching(IsMapContaining.hasKey("k2")));
        assertThat(group.getAllVariables(1, Collections.singleton("k2")))
                .satisfies(matching(IsMapContaining.hasKey("k1")));
        assertThat(group.getAllVariables(1, Collections.singleton("k2")))
                .satisfies(matching(not(IsMapContaining.hasKey("k2"))));
    }

    // ========================================================================
    // Scope Caching
    // ========================================================================

    private static final CharacterFilter FILTER_C =
            new CharacterFilter() {
                @Override
                public String filterCharacters(String input) {
                    return input.replace("C", "X");
                }
            };
    private static final CharacterFilter FILTER_B =
            new CharacterFilter() {
                @Override
                public String filterCharacters(String input) {
                    return input.replace("B", "X");
                }
            };

    @Test
    public void testScopeCachingForMultipleReporters() throws Exception {
        String counterName = "1";
        Configuration config = new Configuration();
        config.setString(MetricOptions.SCOPE_NAMING_TM, "A.B.C.D");

        MetricConfig metricConfig1 = new MetricConfig();
        metricConfig1.setProperty(ConfigConstants.METRICS_REPORTER_SCOPE_DELIMITER, "-");

        MetricConfig metricConfig2 = new MetricConfig();
        metricConfig2.setProperty(ConfigConstants.METRICS_REPORTER_SCOPE_DELIMITER, "!");

        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test1."
                        + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX,
                CollectingMetricsReporter.class.getName());
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test1."
                        + ConfigConstants.METRICS_REPORTER_SCOPE_DELIMITER,
                "-");
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test2."
                        + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX,
                CollectingMetricsReporter.class.getName());
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test2."
                        + ConfigConstants.METRICS_REPORTER_SCOPE_DELIMITER,
                "!");

        CollectingMetricsReporter reporter1 = new CollectingMetricsReporter(FILTER_B);
        CollectingMetricsReporter reporter2 = new CollectingMetricsReporter(FILTER_C);
        MetricRegistryImpl testRegistry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.fromConfiguration(config),
                        Arrays.asList(
                                ReporterSetup.forReporter("test1", metricConfig1, reporter1),
                                ReporterSetup.forReporter("test2", metricConfig2, reporter2)));
        try {
            MetricGroup tmGroup =
                    TaskManagerMetricGroup.createTaskManagerMetricGroup(
                            testRegistry, "host", new ResourceID("id"));
            tmGroup.counter(counterName);
            assertThat(testRegistry.getReporters().size())
                    .as("Reporters were not properly instantiated")
                    .isEqualTo(2);
            {
                // verify reporter1
                MetricGroupAndName nameAndGroup =
                        reporter1.getAddedMetrics().stream()
                                .filter(nag -> nag.name.equals(counterName))
                                .findAny()
                                .get();
                String metricName = nameAndGroup.name;
                MetricGroup group = nameAndGroup.group;

                // the first call determines which filter is applied to all future
                // calls; in
                // this case
                // no filter is used at all
                assertThat(group.getMetricIdentifier(metricName)).isEqualTo("A-B-C-D-1");
                // from now on the scope string is cached and should not be reliant
                // on the
                // given filter
                assertThat(group.getMetricIdentifier(metricName, FILTER_C)).isEqualTo("A-B-C-D-1");
                assertThat(group.getMetricIdentifier(metricName, reporter1)).isEqualTo("A-B-C-D-1");
                // the metric name however is still affected by the filter as it is
                // not
                // cached
                assertThat(
                                group.getMetricIdentifier(
                                        metricName,
                                        input -> input.replace("B", "X").replace(counterName, "4")))
                        .isEqualTo("A-B-C-D-4");
            }
            {
                // verify reporter2
                MetricGroupAndName nameAndGroup =
                        reporter2.getAddedMetrics().stream()
                                .filter(nag -> nag.name.equals(counterName))
                                .findAny()
                                .get();
                String metricName = nameAndGroup.name;
                MetricGroup group = nameAndGroup.group;
                // the first call determines which filter is applied to all future calls
                assertThat(group.getMetricIdentifier(metricName, reporter2)).isEqualTo("A!B!X!D!1");
                // from now on the scope string is cached and should not be reliant on the given
                // filter
                assertThat(group.getMetricIdentifier(metricName)).isEqualTo("A!B!X!D!1");
                assertThat(group.getMetricIdentifier(metricName, FILTER_C)).isEqualTo("A!B!X!D!1");
                // the metric name however is still affected by the filter as it is not cached
                assertThat(
                                group.getMetricIdentifier(
                                        metricName,
                                        input -> input.replace("A", "X").replace(counterName, "3")))
                        .isEqualTo("A!B!X!D!3");
            }
        } finally {
            testRegistry.shutdown().get();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLogicalScopeCachingForMultipleReporters() throws Exception {
        String counterName = "1";
        CollectingMetricsReporter reporter1 = new CollectingMetricsReporter(FILTER_B);
        CollectingMetricsReporter reporter2 = new CollectingMetricsReporter(FILTER_C);
        MetricRegistryImpl testRegistry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration(),
                        Arrays.asList(
                                ReporterSetup.forReporter("test1", reporter1),
                                ReporterSetup.forReporter("test2", reporter2)));
        try {
            MetricGroup tmGroup =
                    TaskManagerMetricGroup.createTaskManagerMetricGroup(
                                    testRegistry, "host", new ResourceID("id"))
                            .addGroup("B")
                            .addGroup("C");
            tmGroup.counter(counterName);
            assertThat(testRegistry.getReporters().size())
                    .as("Reporters were not properly instantiated")
                    .isEqualTo(2);
            assertThat(
                            ((FrontMetricGroup<AbstractMetricGroup<?>>)
                                            reporter1.findAdded(counterName).group)
                                    .getLogicalScope(reporter1, '-'))
                    .isEqualTo("taskmanager-X-C");
            assertThat(
                            ((FrontMetricGroup<AbstractMetricGroup<?>>)
                                            reporter2.findAdded(counterName).group)
                                    .getLogicalScope(reporter2, ','))
                    .isEqualTo("taskmanager,B,X");
        } finally {
            testRegistry.shutdown().get();
        }
    }

    @Test
    public void testScopeGenerationWithoutReporters() throws Exception {
        Configuration config = new Configuration();
        config.setString(MetricOptions.SCOPE_NAMING_TM, "A.B.C.D");
        MetricRegistryImpl testRegistry =
                new MetricRegistryImpl(MetricRegistryTestUtils.fromConfiguration(config));

        try {
            TaskManagerMetricGroup group =
                    TaskManagerMetricGroup.createTaskManagerMetricGroup(
                            testRegistry, "host", new ResourceID("id"));
            assertThat(testRegistry.getReporters().size())
                    .as("MetricReporters list should be empty")
                    .isEqualTo(0);

            // default delimiter should be used
            assertThat(group.getMetricIdentifier("1", FILTER_C)).isEqualTo("A.B.X.D.1");
            // no caching should occur
            assertThat(group.getMetricIdentifier("1", FILTER_B)).isEqualTo("A.X.C.D.1");
            // invalid reporter indices do not throw errors
            assertThat(group.getMetricIdentifier("1", FILTER_B, -1, '.')).isEqualTo("A.X.C.D.1");
            assertThat(group.getMetricIdentifier("1", FILTER_B, 2, '.')).isEqualTo("A.X.C.D.1");
        } finally {
            testRegistry.shutdown().get();
        }
    }

    @Test
    public void testGetAllVariablesDoesNotDeadlock() throws InterruptedException {
        final BlockerSync parentSync = new BlockerSync();
        final BlockerSync childSync = new BlockerSync();

        AtomicReference<BlockerSync> syncRef = new AtomicReference<>();
        final MetricRegistry registry =
                TestingMetricRegistry.builder()
                        .setRegisterConsumer(
                                (metric, metricName, group) -> {
                                    syncRef.get().blockNonInterruptible();
                                    group.getAllVariables();
                                })
                        .build();

        final MetricGroup parent =
                new GenericMetricGroup(
                        registry,
                        UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
                        "parent");
        final MetricGroup child = parent.addGroup("child");

        final Thread parentRegisteringThread = new Thread(() -> parent.counter("parent_counter"));
        final Thread childRegisteringThread = new Thread(() -> child.counter("child_counter"));

        try {
            // start both threads and have them block in the registry, so they acquire the lock of
            // their respective group
            syncRef.set(childSync);
            childRegisteringThread.start();
            childSync.awaitBlocker();

            syncRef.set(parentSync);
            parentRegisteringThread.start();
            parentSync.awaitBlocker();

            // the parent thread remains blocked to simulate the child thread holding some lock in
            // the registry/reporter
            // the child thread continues execution and calls getAllVariables()
            // in the past this would block indefinitely since the method acquires the locks of all
            // parent groups
            childSync.releaseBlocker();
            // wait with a timeout to ensure the finally block is executed _at some point_,
            // un-blocking the parent
            childRegisteringThread.join(1000 * 10);

            parentSync.releaseBlocker();
            parentRegisteringThread.join();
        } finally {
            parentSync.releaseBlocker();
            childSync.releaseBlocker();
            parentRegisteringThread.join();
            childRegisteringThread.join();
        }
    }
}
