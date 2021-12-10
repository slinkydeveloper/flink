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

package org.apache.flink.client.python;

import org.junit.Test;
import py4j.GatewayServer;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link PythonDriver}. */
public class PythonDriverTest {
    @Test
    public void testStartGatewayServer() throws ExecutionException, InterruptedException {
        GatewayServer gatewayServer = PythonEnvUtils.startGatewayServer();
        try {
            Socket socket = new Socket("localhost", gatewayServer.getListeningPort());
            assertThat(socket.isConnected()).isTrue();
        } catch (IOException e) {
            throw new RuntimeException("Connect Gateway Server failed");
        } finally {
            gatewayServer.shutdown();
        }
    }

    @Test
    public void testConstructCommandsWithEntryPointModule() {
        List<String> args = new ArrayList<>();
        args.add("--input");
        args.add("in.txt");

        PythonDriverOptions pythonDriverOptions = new PythonDriverOptions("xxx", null, args);
        List<String> commands = PythonDriver.constructPythonCommands(pythonDriverOptions);
        // verify the generated commands
        assertThat(commands.size()).isEqualTo(4);
        assertThat("-m").isEqualTo(commands.get(0));
        assertThat("xxx").isEqualTo(commands.get(1));
        assertThat("--input").isEqualTo(commands.get(2));
        assertThat("in.txt").isEqualTo(commands.get(3));
    }

    @Test
    public void testConstructCommandsWithEntryPointScript() {
        List<String> args = new ArrayList<>();
        args.add("--input");
        args.add("in.txt");

        PythonDriverOptions pythonDriverOptions = new PythonDriverOptions(null, "xxx", args);
        List<String> commands = PythonDriver.constructPythonCommands(pythonDriverOptions);
        assertThat(commands.size()).isEqualTo(3);
        assertThat("xxx").isEqualTo(commands.get(0));
        assertThat("--input").isEqualTo(commands.get(1));
        assertThat("in.txt").isEqualTo(commands.get(2));
    }
}
