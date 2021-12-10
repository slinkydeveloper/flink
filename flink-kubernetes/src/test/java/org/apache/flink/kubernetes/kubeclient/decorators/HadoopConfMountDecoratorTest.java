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

package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerTestBase;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSource;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KeyToPath;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** General tests for the {@link HadoopConfMountDecorator}. */
public class HadoopConfMountDecoratorTest extends KubernetesJobManagerTestBase {

    private static final String EXISTING_HADOOP_CONF_CONFIG_MAP = "hadoop-conf";

    private HadoopConfMountDecorator hadoopConfMountDecorator;

    @Override
    protected void onSetup() throws Exception {
        super.onSetup();

        this.hadoopConfMountDecorator =
                new HadoopConfMountDecorator(kubernetesJobManagerParameters);
    }

    @Test
    public void testExistingHadoopConfigMap() throws IOException {
        flinkConfig.set(
                KubernetesConfigOptions.HADOOP_CONF_CONFIG_MAP, EXISTING_HADOOP_CONF_CONFIG_MAP);
        assertThat(hadoopConfMountDecorator.buildAccompanyingKubernetesResources().size())
                .isEqualTo(0);

        final FlinkPod resultFlinkPod = hadoopConfMountDecorator.decorateFlinkPod(baseFlinkPod);
        final List<Volume> volumes =
                resultFlinkPod.getPodWithoutMainContainer().getSpec().getVolumes();
        assertThat(
                        volumes.stream()
                                .anyMatch(
                                        volume ->
                                                volume.getConfigMap()
                                                        .getName()
                                                        .equals(EXISTING_HADOOP_CONF_CONFIG_MAP)))
                .isTrue();
    }

    @Test
    public void testExistingConfigMapPrecedeOverHadoopConfEnv() throws IOException {
        // set existing ConfigMap
        flinkConfig.set(
                KubernetesConfigOptions.HADOOP_CONF_CONFIG_MAP, EXISTING_HADOOP_CONF_CONFIG_MAP);

        // set HADOOP_CONF_DIR
        setHadoopConfDirEnv();
        generateHadoopConfFileItems();

        assertThat(hadoopConfMountDecorator.buildAccompanyingKubernetesResources().size())
                .isEqualTo(0);

        final FlinkPod resultFlinkPod = hadoopConfMountDecorator.decorateFlinkPod(baseFlinkPod);
        final List<Volume> volumes =
                resultFlinkPod.getPodWithoutMainContainer().getSpec().getVolumes();
        assertThat(
                        volumes.stream()
                                .anyMatch(
                                        volume ->
                                                volume.getConfigMap()
                                                        .getName()
                                                        .equals(EXISTING_HADOOP_CONF_CONFIG_MAP)))
                .isTrue();
        assertThat(
                        volumes.stream()
                                .anyMatch(
                                        volume ->
                                                volume.getConfigMap()
                                                        .getName()
                                                        .equals(
                                                                HadoopConfMountDecorator
                                                                        .getHadoopConfConfigMapName(
                                                                                CLUSTER_ID))))
                .isFalse();
    }

    @Test
    public void testHadoopConfDirectoryUnset() throws IOException {
        assertThat(hadoopConfMountDecorator.buildAccompanyingKubernetesResources().size())
                .isEqualTo(0);

        final FlinkPod resultFlinkPod = hadoopConfMountDecorator.decorateFlinkPod(baseFlinkPod);
        assertThat(resultFlinkPod.getPodWithoutMainContainer())
                .isEqualTo(baseFlinkPod.getPodWithoutMainContainer());
        assertThat(resultFlinkPod.getMainContainer()).isEqualTo(baseFlinkPod.getMainContainer());
    }

    @Test
    public void testEmptyHadoopConfDirectory() throws IOException {
        setHadoopConfDirEnv();

        assertThat(hadoopConfMountDecorator.buildAccompanyingKubernetesResources().size())
                .isEqualTo(0);

        final FlinkPod resultFlinkPod = hadoopConfMountDecorator.decorateFlinkPod(baseFlinkPod);
        assertThat(resultFlinkPod.getPodWithoutMainContainer())
                .isEqualTo(baseFlinkPod.getPodWithoutMainContainer());
        assertThat(resultFlinkPod.getMainContainer()).isEqualTo(baseFlinkPod.getMainContainer());
    }

    @Test
    public void testHadoopConfConfigMap() throws IOException {
        setHadoopConfDirEnv();
        generateHadoopConfFileItems();

        final List<HasMetadata> additionalResources =
                hadoopConfMountDecorator.buildAccompanyingKubernetesResources();
        assertThat(additionalResources.size()).isEqualTo(1);

        final ConfigMap resultConfigMap = (ConfigMap) additionalResources.get(0);

        assertThat(resultConfigMap.getApiVersion()).isEqualTo(Constants.API_VERSION);
        assertThat(resultConfigMap.getMetadata().getName())
                .isEqualTo(HadoopConfMountDecorator.getHadoopConfConfigMapName(CLUSTER_ID));
        assertThat(resultConfigMap.getMetadata().getLabels()).isEqualTo(getCommonLabels());

        Map<String, String> resultDatas = resultConfigMap.getData();
        assertThat(resultDatas.get("core-site.xml")).isEqualTo("some data");
        assertThat(resultDatas.get("hdfs-site.xml")).isEqualTo("some data");
    }

    @Test
    public void testPodWithHadoopConfVolume() throws IOException {
        setHadoopConfDirEnv();
        generateHadoopConfFileItems();
        final FlinkPod resultFlinkPod = hadoopConfMountDecorator.decorateFlinkPod(baseFlinkPod);

        final List<Volume> resultVolumes =
                resultFlinkPod.getPodWithoutMainContainer().getSpec().getVolumes();
        assertThat(resultVolumes.size()).isEqualTo(1);

        final Volume resultVolume = resultVolumes.get(0);
        assertThat(resultVolume.getName()).isEqualTo(Constants.HADOOP_CONF_VOLUME);

        final ConfigMapVolumeSource resultVolumeConfigMap = resultVolume.getConfigMap();
        assertThat(resultVolumeConfigMap.getName())
                .isEqualTo(HadoopConfMountDecorator.getHadoopConfConfigMapName(CLUSTER_ID));

        final Map<String, String> expectedKeyToPaths =
                new HashMap<String, String>() {
                    {
                        put("hdfs-site.xml", "hdfs-site.xml");
                        put("core-site.xml", "core-site.xml");
                    }
                };
        final Map<String, String> resultKeyToPaths =
                resultVolumeConfigMap.getItems().stream()
                        .collect(Collectors.toMap(KeyToPath::getKey, KeyToPath::getPath));
        assertThat(resultKeyToPaths).isEqualTo(expectedKeyToPaths);
    }

    @Test
    public void testMainContainerWithHadoopConfVolumeMount() throws IOException {
        setHadoopConfDirEnv();
        generateHadoopConfFileItems();
        final FlinkPod resultFlinkPod = hadoopConfMountDecorator.decorateFlinkPod(baseFlinkPod);

        final List<VolumeMount> resultVolumeMounts =
                resultFlinkPod.getMainContainer().getVolumeMounts();
        assertThat(resultVolumeMounts.size()).isEqualTo(1);
        final VolumeMount resultVolumeMount = resultVolumeMounts.get(0);
        assertThat(resultVolumeMount.getName()).isEqualTo(Constants.HADOOP_CONF_VOLUME);
        assertThat(resultVolumeMount.getMountPath()).isEqualTo(Constants.HADOOP_CONF_DIR_IN_POD);

        final Map<String, String> expectedEnvs =
                new HashMap<String, String>() {
                    {
                        put(Constants.ENV_HADOOP_CONF_DIR, Constants.HADOOP_CONF_DIR_IN_POD);
                    }
                };
        final Map<String, String> resultEnvs =
                resultFlinkPod.getMainContainer().getEnv().stream()
                        .collect(Collectors.toMap(EnvVar::getName, EnvVar::getValue));
        assertThat(resultEnvs).isEqualTo(expectedEnvs);
    }
}
