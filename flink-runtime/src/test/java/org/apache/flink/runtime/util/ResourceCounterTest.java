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

package org.apache.flink.runtime.util;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.HamcrestCondition.matching;

/** Tests for the {@link ResourceCounter}. */
public class ResourceCounterTest extends TestLogger {

    private ResourceProfile resourceProfile1 =
            ResourceProfile.newBuilder().setManagedMemoryMB(42).build();
    private ResourceProfile resourceProfile2 =
            ResourceProfile.newBuilder().setCpuCores(1.7).build();

    @Test
    public void testIsEmpty() {
        final ResourceCounter empty = ResourceCounter.empty();

        assertThat(empty.isEmpty()).isTrue();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithResourceRejectsNegativeCount() {
        ResourceCounter.withResource(ResourceProfile.UNKNOWN, -1);
    }

    @Test
    public void testWithResourceCreatesEmptyCounterIfCountIsZero() {
        final ResourceCounter empty = ResourceCounter.withResource(ResourceProfile.UNKNOWN, 0);

        assertThat(empty.isEmpty()).isTrue();
    }

    @Test
    public void testIsNonEmpty() {
        final ResourceCounter resourceCounter =
                ResourceCounter.withResource(ResourceProfile.UNKNOWN, 1);

        assertThat(resourceCounter.isEmpty()).isFalse();
        assertThat(resourceCounter.containsResource(ResourceProfile.UNKNOWN)).isTrue();
    }

    @Test
    public void testGetResourceCount() {
        final Map<ResourceProfile, Integer> resources = createResources();

        final ResourceCounter resourceCounter = ResourceCounter.withResources(resources);

        for (Map.Entry<ResourceProfile, Integer> resource : resources.entrySet()) {
            assertThat(resourceCounter.getResourceCount(resource.getKey()))
                    .isEqualTo(resource.getValue());
        }
    }

    @Test
    public void testGetResourceCountReturnsZeroForUnknownResourceProfile() {
        final ResourceCounter resourceCounter = ResourceCounter.withResources(createResources());

        assertThat(resourceCounter.getResourceCount(ResourceProfile.newBuilder().build()))
                .isEqualTo(0);
    }

    @Test
    public void testGetTotalResourceCount() {
        final Map<ResourceProfile, Integer> resources = createResources();

        final ResourceCounter resourceCounter = ResourceCounter.withResources(resources);

        assertThat(resourceCounter.getTotalResourceCount()).isEqualTo(5);
    }

    @Test
    public void testGetResources() {
        final Map<ResourceProfile, Integer> resources = createResources();
        final ResourceCounter resourceCounter = ResourceCounter.withResources(resources);

        assertThat(resourceCounter.getResources())
                .satisfies(matching(Matchers.containsInAnyOrder(resources.keySet().toArray())));
    }

    @Test
    public void testGetResourceWithCount() {
        final Map<ResourceProfile, Integer> resources = createResources();
        final ResourceCounter resourceCounter = ResourceCounter.withResources(resources);

        assertThat(resourceCounter.getResourcesWithCount())
                .satisfies(matching(Matchers.containsInAnyOrder(resources.entrySet().toArray())));
    }

    @Test
    public void testAddSameResourceProfile() {
        final int value1 = 1;
        final int value2 = 42;

        final ResourceCounter resourceCounter1 =
                ResourceCounter.withResource(ResourceProfile.UNKNOWN, value1);
        final ResourceCounter resourceCounter2 =
                ResourceCounter.withResource(ResourceProfile.UNKNOWN, value2);

        final ResourceCounter result = resourceCounter1.add(resourceCounter2);

        assertThat(resourceCounter1.getResourcesWithCount())
                .satisfies(
                        matching(
                                Matchers.containsInAnyOrder(
                                        Collections.singletonMap(ResourceProfile.UNKNOWN, value1)
                                                .entrySet()
                                                .toArray())));
        assertThat(resourceCounter2.getResourcesWithCount())
                .satisfies(
                        matching(
                                Matchers.containsInAnyOrder(
                                        Collections.singletonMap(ResourceProfile.UNKNOWN, value2)
                                                .entrySet()
                                                .toArray())));

        assertThat(result.getResourcesWithCount())
                .satisfies(
                        matching(
                                Matchers.containsInAnyOrder(
                                        Collections.singletonMap(
                                                        ResourceProfile.UNKNOWN, value1 + value2)
                                                .entrySet()
                                                .toArray())));
    }

    @Test
    public void testAddDifferentResourceProfile() {
        final ResourceCounter resourceCounter1 = ResourceCounter.withResource(resourceProfile1, 1);
        final ResourceCounter resourceCounter2 = ResourceCounter.withResource(resourceProfile2, 1);

        final ResourceCounter result = resourceCounter1.add(resourceCounter2);

        final Collection<Map.Entry<ResourceProfile, Integer>> expectedResult =
                new ArrayList<>(resourceCounter1.getResourcesWithCount());
        expectedResult.addAll(resourceCounter2.getResourcesWithCount());

        assertThat(result.getResourcesWithCount())
                .satisfies(matching(Matchers.containsInAnyOrder(expectedResult.toArray())));
    }

    @Test
    public void testCountEqualToZeroRemovesResource() {
        final ResourceCounter resourceCounter = ResourceCounter.withResource(resourceProfile1, 2);

        final ResourceCounter result = resourceCounter.subtract(resourceProfile1, 2);

        assertThat(result.isEmpty()).isTrue();
    }

    @Test
    public void testCountBelowZeroRemovesResources() {
        final ResourceCounter resourceCounter = ResourceCounter.withResource(resourceProfile1, 1);

        final ResourceCounter result = resourceCounter.subtract(resourceProfile1, 2);

        assertThat(result.isEmpty()).isTrue();
    }

    @Test
    public void testSubtractSameResourceProfile() {
        final int value1 = 5;
        final int value2 = 3;

        final ResourceCounter resourceCounter1 =
                ResourceCounter.withResource(ResourceProfile.UNKNOWN, value1);
        final ResourceCounter resourceCounter2 =
                ResourceCounter.withResource(ResourceProfile.UNKNOWN, value2);

        final ResourceCounter result = resourceCounter1.subtract(resourceCounter2);

        assertThat(resourceCounter1.getResourcesWithCount())
                .satisfies(
                        matching(
                                Matchers.containsInAnyOrder(
                                        Collections.singletonMap(ResourceProfile.UNKNOWN, value1)
                                                .entrySet()
                                                .toArray())));
        assertThat(resourceCounter2.getResourcesWithCount())
                .satisfies(
                        matching(
                                Matchers.containsInAnyOrder(
                                        Collections.singletonMap(ResourceProfile.UNKNOWN, value2)
                                                .entrySet()
                                                .toArray())));

        assertThat(result.getResourcesWithCount())
                .satisfies(
                        matching(
                                Matchers.containsInAnyOrder(
                                        Collections.singletonMap(
                                                        ResourceProfile.UNKNOWN, value1 - value2)
                                                .entrySet()
                                                .toArray())));
    }

    @Test
    public void testSubtractDifferentResourceProfile() {
        final ResourceCounter resourceCounter1 = ResourceCounter.withResource(resourceProfile1, 1);
        final ResourceCounter resourceCounter2 = ResourceCounter.withResource(resourceProfile2, 1);

        final ResourceCounter result = resourceCounter1.subtract(resourceCounter2);

        assertThat(result.getResourcesWithCount())
                .satisfies(
                        matching(
                                Matchers.containsInAnyOrder(
                                        resourceCounter1.getResourcesWithCount().toArray())));
    }

    private Map<ResourceProfile, Integer> createResources() {
        return ImmutableMap.of(
                resourceProfile1, 2,
                resourceProfile2, 3);
    }
}
