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

package org.apache.flink.runtime.rest;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.io.network.netty.InboundChannelHandlerFactory;
import org.apache.flink.runtime.io.network.netty.OutboundChannelHandlerFactory;
import org.apache.flink.runtime.io.network.netty.Prio0InboundChannelHandlerFactory;
import org.apache.flink.runtime.io.network.netty.Prio1InboundChannelHandlerFactory;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.util.TestRestServerEndpoint;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.junit.extensions.ContextClassLoaderExtension;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** IT cases for {@link RestClient} and {@link RestServerEndpoint}. */
public class RestExternalHandlersITCase extends TestLogger {

    private static final Time timeout = Time.seconds(10L);
    private static final String REQUEST_URL = "/nonExisting1";
    private static final String REDIRECT1_URL = "/nonExisting2";
    private static final String REDIRECT2_URL = "/nonExisting3";

    private RestServerEndpoint serverEndpoint;
    private RestClient restClient;
    private InetSocketAddress serverAddress;

    @RegisterExtension
    static final Extension CONTEXT_CLASS_LOADER_EXTENSION =
            ContextClassLoaderExtension.builder()
                    .withServiceEntry(
                            InboundChannelHandlerFactory.class,
                            Prio0InboundChannelHandlerFactory.class.getCanonicalName(),
                            Prio1InboundChannelHandlerFactory.class.getCanonicalName())
                    .withServiceEntry(
                            OutboundChannelHandlerFactory.class,
                            Prio0OutboundChannelHandlerFactory.class.getCanonicalName(),
                            Prio1OutboundChannelHandlerFactory.class.getCanonicalName())
                    .build();

    private final Configuration config;

    public RestExternalHandlersITCase() {
        this.config = getBaseConfig();
    }

    private static Configuration getBaseConfig() {
        final String loopbackAddress = InetAddress.getLoopbackAddress().getHostAddress();

        final Configuration config = new Configuration();
        config.setString(RestOptions.BIND_PORT, "0");
        config.setString(RestOptions.BIND_ADDRESS, loopbackAddress);
        config.setString(RestOptions.ADDRESS, loopbackAddress);
        config.setString(Prio0OutboundChannelHandlerFactory.REDIRECT_TO_URL, REDIRECT1_URL);
        config.setString(Prio0InboundChannelHandlerFactory.REDIRECT_FROM_URL, REDIRECT1_URL);
        config.setString(Prio0InboundChannelHandlerFactory.REDIRECT_TO_URL, REDIRECT2_URL);
        return config;
    }

    @BeforeEach
    private void setup() throws Exception {
        serverEndpoint = TestRestServerEndpoint.builder(config).buildAndStart();
        restClient = new TestRestClient(config);
        serverAddress = serverEndpoint.getServerAddress();
    }

    @AfterEach
    private void teardown() throws Exception {
        if (restClient != null) {
            restClient.shutdown(timeout);
            restClient = null;
        }

        if (serverEndpoint != null) {
            serverEndpoint.closeAsync().get(timeout.getSize(), timeout.getUnit());
            serverEndpoint = null;
        }
    }

    @Test
    void testHandlersMustBeLoaded() throws Exception {
        assertThat(2).isEqualTo(serverEndpoint.inboundChannelHandlerFactories.size());
        assertThat(serverEndpoint.inboundChannelHandlerFactories.get(0))
                .isInstanceOf(Prio1InboundChannelHandlerFactory.class);
        assertThat(serverEndpoint.inboundChannelHandlerFactories.get(1))
                .isInstanceOf(Prio0InboundChannelHandlerFactory.class);

        assertThat(2).isEqualTo(restClient.outboundChannelHandlerFactories.size());
        assertThat(restClient.outboundChannelHandlerFactories.get(0))
                .isInstanceOf(Prio1OutboundChannelHandlerFactory.class);
        assertThat(restClient.outboundChannelHandlerFactories.get(1))
                .isInstanceOf(Prio0OutboundChannelHandlerFactory.class);

        try {
            final CompletableFuture<TestResponse> response =
                    sendRequestToTestHandler(new TestRequest());
            response.get();
            fail("Request must fail with 2 times redirected URL");
        } catch (Exception e) {
            assertThat(e.getMessage().contains(REDIRECT2_URL)).isTrue();
        }
    }

    private CompletableFuture<TestResponse> sendRequestToTestHandler(
            final TestRequest testRequest) {
        try {
            return restClient.sendRequest(
                    serverAddress.getHostName(),
                    serverAddress.getPort(),
                    new TestHeaders(),
                    EmptyMessageParameters.getInstance(),
                    testRequest);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    static class TestRestClient extends RestClient {

        TestRestClient(Configuration configuration) throws ConfigurationException {
            super(configuration, TestingUtils.defaultExecutor());
        }
    }

    private static class TestRequest implements RequestBody {}

    private static class TestResponse implements ResponseBody {}

    private static class TestHeaders
            implements MessageHeaders<TestRequest, TestResponse, EmptyMessageParameters> {

        @Override
        public HttpMethodWrapper getHttpMethod() {
            return HttpMethodWrapper.POST;
        }

        @Override
        public String getTargetRestEndpointURL() {
            return REQUEST_URL;
        }

        @Override
        public Class<TestRequest> getRequestClass() {
            return TestRequest.class;
        }

        @Override
        public Class<TestResponse> getResponseClass() {
            return TestResponse.class;
        }

        @Override
        public HttpResponseStatus getResponseStatusCode() {
            return HttpResponseStatus.OK;
        }

        @Override
        public String getDescription() {
            return "";
        }

        @Override
        public EmptyMessageParameters getUnresolvedMessageParameters() {
            return EmptyMessageParameters.getInstance();
        }
    }
}
