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

package org.apache.flink.kubernetes.kubeclient.resources;

import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.util.TestLogger;

import io.fabric8.kubernetes.api.model.StatusBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static java.net.HttpURLConnection.HTTP_GONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.HamcrestCondition.matching;

/** Tests for {@link KubernetesPodsWatcher}. */
public class KubernetesPodsWatcherTest extends TestLogger {

    private final List<KubernetesPod> podAddedList = new ArrayList<>();
    private final List<KubernetesPod> podModifiedList = new ArrayList<>();
    private final List<KubernetesPod> podDeletedList = new ArrayList<>();
    private final List<KubernetesPod> podErrorList = new ArrayList<>();

    @Test
    public void testClosingWithNullException() {
        final KubernetesPodsWatcher podsWatcher =
                new KubernetesPodsWatcher(
                        new TestingCallbackHandler(e -> fail("Should not reach here.")));
        podsWatcher.onClose(null);
    }

    @Test
    public void testClosingWithException() {
        final AtomicBoolean called = new AtomicBoolean(false);
        final KubernetesPodsWatcher podsWatcher =
                new KubernetesPodsWatcher(new TestingCallbackHandler(e -> called.set(true)));
        podsWatcher.onClose(new WatcherException("exception"));
        assertThat(called.get()).isEqualTo(true);
    }

    @Test
    public void testCallbackHandler() {
        FlinkPod pod = new FlinkPod.Builder().build();
        final KubernetesPodsWatcher podsWatcher =
                new KubernetesPodsWatcher(new TestingCallbackHandler(e -> {}));
        podsWatcher.eventReceived(Watcher.Action.ADDED, pod.getPodWithoutMainContainer());
        podsWatcher.eventReceived(Watcher.Action.MODIFIED, pod.getPodWithoutMainContainer());
        podsWatcher.eventReceived(Watcher.Action.DELETED, pod.getPodWithoutMainContainer());
        podsWatcher.eventReceived(Watcher.Action.ERROR, pod.getPodWithoutMainContainer());

        assertThat(podAddedList.size()).isEqualTo(1);
        assertThat(podModifiedList.size()).isEqualTo(1);
        assertThat(podDeletedList.size()).isEqualTo(1);
        assertThat(podErrorList.size()).isEqualTo(1);
    }

    @Test
    public void testClosingWithTooOldResourceVersion() {
        final String errMsg = "too old resource version";
        final KubernetesPodsWatcher podsWatcher =
                new KubernetesPodsWatcher(
                        new TestingCallbackHandler(
                                e -> {
                                    assertThat(e)
                                            .isInstanceOf(
                                                    KubernetesTooOldResourceVersionException.class);
                                    assertThat(e)
                                            .satisfies(
                                                    matching(
                                                            FlinkMatchers.containsMessage(errMsg)));
                                }));
        podsWatcher.onClose(
                new WatcherException(
                        errMsg,
                        new KubernetesClientException(
                                errMsg, HTTP_GONE, new StatusBuilder().build())));
    }

    private class TestingCallbackHandler
            implements FlinkKubeClient.WatchCallbackHandler<KubernetesPod> {

        final Consumer<Throwable> consumer;

        TestingCallbackHandler(Consumer<Throwable> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void onAdded(List<KubernetesPod> pods) {
            podAddedList.addAll(pods);
        }

        @Override
        public void onModified(List<KubernetesPod> pods) {
            podModifiedList.addAll(pods);
        }

        @Override
        public void onDeleted(List<KubernetesPod> pods) {
            podDeletedList.addAll(pods);
        }

        @Override
        public void onError(List<KubernetesPod> pods) {
            podErrorList.addAll(pods);
        }

        @Override
        public void handleError(Throwable throwable) {
            consumer.accept(throwable);
        }
    }
}
