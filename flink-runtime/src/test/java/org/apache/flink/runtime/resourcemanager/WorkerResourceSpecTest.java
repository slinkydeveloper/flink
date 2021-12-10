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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.resources.ExternalResource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalResourceOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link WorkerResourceSpec}. */
public class WorkerResourceSpecTest extends TestLogger {
    private static final String EXTERNAL_RESOURCE_NAME = "gpu";

    @Test
    public void testEquals() {
        final WorkerResourceSpec spec1 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec2 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec3 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.1)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec4 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(110)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec5 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(110)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec6 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(110)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec7 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(110)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec8 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(2)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec9 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 2))
                        .build();
        final WorkerResourceSpec spec10 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .build();

        assertThat(spec1).isEqualTo(spec1);
        assertThat(spec2).isEqualTo(spec1);
        assertThat(spec3).isEqualTo(spec1);
        assertThat(spec4).isEqualTo(spec1);
        assertThat(spec5).isEqualTo(spec1);
        assertThat(spec6).isEqualTo(spec1);
        assertThat(spec7).isEqualTo(spec1);
        assertThat(spec8).isEqualTo(spec1);
        assertThat(spec9).isEqualTo(spec1);
        assertThat(spec10).isEqualTo(spec1);
    }

    @Test
    public void testHashCodeEquals() {
        final WorkerResourceSpec spec1 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec2 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec3 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.1)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec4 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(110)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec5 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(110)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec6 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(110)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec7 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(110)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec8 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(2)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec spec9 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 2))
                        .build();
        final WorkerResourceSpec spec10 =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setManagedMemoryMB(100)
                        .setNumSlots(1)
                        .build();

        assertThat(spec1.hashCode()).isEqualTo(spec1.hashCode());
        assertThat(spec2.hashCode()).isEqualTo(spec1.hashCode());
        assertThat(spec3.hashCode()).isEqualTo(spec1.hashCode());
        assertThat(spec4.hashCode()).isEqualTo(spec1.hashCode());
        assertThat(spec5.hashCode()).isEqualTo(spec1.hashCode());
        assertThat(spec6.hashCode()).isEqualTo(spec1.hashCode());
        assertThat(spec7.hashCode()).isEqualTo(spec1.hashCode());
        assertThat(spec8.hashCode()).isEqualTo(spec1.hashCode());
        assertThat(spec9.hashCode()).isEqualTo(spec1.hashCode());
        assertThat(spec10.hashCode()).isEqualTo(spec1.hashCode());
    }

    @Test
    public void testCreateFromTaskExecutorProcessSpec() {
        final Configuration config = new Configuration();
        config.setString(
                ExternalResourceOptions.EXTERNAL_RESOURCE_LIST.key(), EXTERNAL_RESOURCE_NAME);
        config.setLong(
                ExternalResourceOptions.getAmountConfigOptionForResource(EXTERNAL_RESOURCE_NAME),
                1);

        final TaskExecutorProcessSpec taskExecutorProcessSpec =
                TaskExecutorProcessUtils.newProcessSpecBuilder(config)
                        .withTotalProcessMemory(MemorySize.ofMebiBytes(1024))
                        .build();
        final WorkerResourceSpec workerResourceSpec =
                WorkerResourceSpec.fromTaskExecutorProcessSpec(taskExecutorProcessSpec);
        assertThat(taskExecutorProcessSpec.getCpuCores())
                .isEqualTo(workerResourceSpec.getCpuCores());
        assertThat(taskExecutorProcessSpec.getTaskHeapSize())
                .isEqualTo(workerResourceSpec.getTaskHeapSize());
        assertThat(taskExecutorProcessSpec.getTaskOffHeapSize())
                .isEqualTo(workerResourceSpec.getTaskOffHeapSize());
        assertThat(taskExecutorProcessSpec.getNetworkMemSize())
                .isEqualTo(workerResourceSpec.getNetworkMemSize());
        assertThat(taskExecutorProcessSpec.getManagedMemorySize())
                .isEqualTo(workerResourceSpec.getManagedMemSize());
        assertThat(taskExecutorProcessSpec.getNumSlots())
                .isEqualTo(workerResourceSpec.getNumSlots());
        assertThat(taskExecutorProcessSpec.getExtendedResources())
                .isEqualTo(workerResourceSpec.getExtendedResources());
    }

    @Test
    public void testCreateFromResourceProfile() {
        final int numSlots = 3;
        final ResourceProfile resourceProfile =
                ResourceProfile.newBuilder()
                        .setCpuCores(1)
                        .setManagedMemoryMB(100)
                        .setNetworkMemoryMB(100)
                        .setTaskOffHeapMemoryMB(10)
                        .setTaskHeapMemoryMB(10)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec workerResourceSpec =
                WorkerResourceSpec.fromTotalResourceProfile(resourceProfile, numSlots);
        assertThat(resourceProfile.getCpuCores()).isEqualTo(workerResourceSpec.getCpuCores());
        assertThat(resourceProfile.getTaskHeapMemory())
                .isEqualTo(workerResourceSpec.getTaskHeapSize());
        assertThat(resourceProfile.getTaskOffHeapMemory())
                .isEqualTo(workerResourceSpec.getTaskOffHeapSize());
        assertThat(resourceProfile.getNetworkMemory())
                .isEqualTo(workerResourceSpec.getNetworkMemSize());
        assertThat(resourceProfile.getManagedMemory())
                .isEqualTo(workerResourceSpec.getManagedMemSize());
        assertThat(numSlots).isEqualTo(workerResourceSpec.getNumSlots());
        assertThat(resourceProfile.getExtendedResources())
                .isEqualTo(workerResourceSpec.getExtendedResources());
    }
}
