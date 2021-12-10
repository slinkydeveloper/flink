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

package org.apache.flink.util;

import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.HamcrestCondition.matching;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.hamcrest.core.IsNot.not;

/** Tests for the {@link NetUtils}. */
public class NetUtilsTest extends TestLogger {

    @Test
    public void testCorrectHostnamePort() throws Exception {
        final URL url = new URL("http", "foo.com", 8080, "/index.html");
        assertThat(NetUtils.getCorrectHostnamePort("foo.com:8080/index.html")).isEqualTo(url);
    }

    @Test
    public void testParseHostPortAddress() {
        final InetSocketAddress socketAddress = new InetSocketAddress("foo.com", 8080);
        assertThat(NetUtils.parseHostPortAddress("foo.com:8080")).isEqualTo(socketAddress);
    }

    @Test
    public void testAcceptWithoutTimeoutSuppressesTimeoutException() throws IOException {
        // Validates that acceptWithoutTimeout suppresses all SocketTimeoutExceptions
        Socket expected = new Socket();
        ServerSocket serverSocket =
                new ServerSocket() {
                    private int count = 0;

                    @Override
                    public Socket accept() throws IOException {
                        if (count < 2) {
                            count++;
                            throw new SocketTimeoutException();
                        }

                        return expected;
                    }
                };

        assertThat(NetUtils.acceptWithoutTimeout(serverSocket)).isEqualTo(expected);
    }

    @Test
    public void testAcceptWithoutTimeoutDefaultTimeout() throws IOException {
        // Default timeout (should be zero)
        final Socket expected = new Socket();
        try (final ServerSocket serverSocket =
                new ServerSocket(0) {
                    @Override
                    public Socket accept() {
                        return expected;
                    }
                }) {
            assertThat(NetUtils.acceptWithoutTimeout(serverSocket)).isEqualTo(expected);
        }
    }

    @Test
    public void testAcceptWithoutTimeoutZeroTimeout() throws IOException {
        // Explicitly sets a timeout of zero
        final Socket expected = new Socket();
        try (final ServerSocket serverSocket =
                new ServerSocket(0) {
                    @Override
                    public Socket accept() {
                        return expected;
                    }
                }) {
            serverSocket.setSoTimeout(0);
            assertThat(NetUtils.acceptWithoutTimeout(serverSocket)).isEqualTo(expected);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAcceptWithoutTimeoutRejectsSocketWithSoTimeout() throws IOException {
        try (final ServerSocket serverSocket = new ServerSocket(0)) {
            serverSocket.setSoTimeout(5);
            NetUtils.acceptWithoutTimeout(serverSocket);
        }
    }

    @Test
    public void testIPv4toURL() {
        try {
            final String addressString = "192.168.0.1";

            InetAddress address = InetAddress.getByName(addressString);
            assertThat(NetUtils.ipAddressToUrlString(address)).isEqualTo(addressString);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testIPv6toURL() {
        try {
            final String addressString = "2001:01db8:00:0:00:ff00:42:8329";
            final String normalizedAddress = "[2001:1db8::ff00:42:8329]";

            InetAddress address = InetAddress.getByName(addressString);
            assertThat(NetUtils.ipAddressToUrlString(address)).isEqualTo(normalizedAddress);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testIPv4URLEncoding() {
        try {
            final String addressString = "10.244.243.12";
            final int port = 23453;

            InetAddress address = InetAddress.getByName(addressString);
            InetSocketAddress socketAddress = new InetSocketAddress(address, port);

            assertThat(NetUtils.ipAddressToUrlString(address)).isEqualTo(addressString);
            assertThat(NetUtils.ipAddressAndPortToUrlString(address, port))
                    .isEqualTo(addressString + ':' + port);
            assertThat(NetUtils.socketAddressToUrlString(socketAddress))
                    .isEqualTo(addressString + ':' + port);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testIPv6URLEncoding() {
        try {
            final String addressString = "2001:db8:10:11:12:ff00:42:8329";
            final String bracketedAddressString = '[' + addressString + ']';
            final int port = 23453;

            InetAddress address = InetAddress.getByName(addressString);
            InetSocketAddress socketAddress = new InetSocketAddress(address, port);

            assertThat(NetUtils.ipAddressToUrlString(address)).isEqualTo(bracketedAddressString);
            assertThat(NetUtils.ipAddressAndPortToUrlString(address, port))
                    .isEqualTo(bracketedAddressString + ':' + port);
            assertThat(NetUtils.socketAddressToUrlString(socketAddress))
                    .isEqualTo(bracketedAddressString + ':' + port);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testFreePortRangeUtility() {
        // inspired by Hadoop's example for "yarn.app.mapreduce.am.job.client.port-range"
        String rangeDefinition =
                "50000-50050, 50100-50200,51234 "; // this also contains some whitespaces
        Iterator<Integer> portsIter = NetUtils.getPortRangeFromString(rangeDefinition);
        Set<Integer> ports = new HashSet<>();
        while (portsIter.hasNext()) {
            assertThat(ports.add(portsIter.next())).as("Duplicate element").isTrue();
        }

        assertThat(ports.size()).isEqualTo(51 + 101 + 1);
        // check first range
        assertThat(ports).satisfies(matching(hasItems(50000, 50001, 50002, 50050)));
        // check second range and last point
        assertThat(ports).satisfies(matching(hasItems(50100, 50101, 50110, 50200, 51234)));
        // check that only ranges are included
        assertThat(ports)
                .satisfies(matching(not(hasItems(50051, 50052, 1337, 50201, 49999, 50099))));

        // test single port "range":
        portsIter = NetUtils.getPortRangeFromString(" 51234");
        assertThat(portsIter.hasNext()).isTrue();
        assertThat((int) portsIter.next()).isEqualTo(51234);
        assertThat(portsIter.hasNext()).isFalse();

        // test port list
        portsIter = NetUtils.getPortRangeFromString("5,1,2,3,4");
        assertThat(portsIter.hasNext()).isTrue();
        assertThat((int) portsIter.next()).isEqualTo(5);
        assertThat((int) portsIter.next()).isEqualTo(1);
        assertThat((int) portsIter.next()).isEqualTo(2);
        assertThat((int) portsIter.next()).isEqualTo(3);
        assertThat((int) portsIter.next()).isEqualTo(4);
        assertThat(portsIter.hasNext()).isFalse();

        Throwable error = null;

        // try some wrong values: String
        try {
            NetUtils.getPortRangeFromString("localhost");
        } catch (Throwable t) {
            error = t;
        }
        assertThat(error).isInstanceOf(NumberFormatException.class);
        error = null;

        // incomplete range
        try {
            NetUtils.getPortRangeFromString("5-");
        } catch (Throwable t) {
            error = t;
        }
        assertThat(error).isInstanceOf(NumberFormatException.class);
        error = null;

        // incomplete range
        try {
            NetUtils.getPortRangeFromString("-5");
        } catch (Throwable t) {
            error = t;
        }
        assertThat(error).isInstanceOf(NumberFormatException.class);
        error = null;

        // empty range
        try {
            NetUtils.getPortRangeFromString(",5");
        } catch (Throwable t) {
            error = t;
        }
        assertThat(error).isInstanceOf(NumberFormatException.class);
    }

    @Test
    public void testFormatAddress() throws UnknownHostException {
        {
            // IPv4
            String host = "1.2.3.4";
            int port = 42;
            assertThat(NetUtils.unresolvedHostAndPortToNormalizedString(host, port))
                    .isEqualTo(host + ":" + port);
        }
        {
            // IPv6
            String host = "2001:0db8:85a3:0000:0000:8a2e:0370:7334";
            int port = 42;
            assertThat(NetUtils.unresolvedHostAndPortToNormalizedString(host, port))
                    .isEqualTo("[2001:db8:85a3::8a2e:370:7334]:" + port);
        }
        {
            // [IPv6]
            String host = "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]";
            int port = 42;
            assertThat(NetUtils.unresolvedHostAndPortToNormalizedString(host, port))
                    .isEqualTo("[2001:db8:85a3::8a2e:370:7334]:" + port);
        }
        {
            // Hostnames
            String host = "somerandomhostname";
            int port = 99;
            assertThat(NetUtils.unresolvedHostAndPortToNormalizedString(host, port))
                    .isEqualTo(host + ":" + port);
        }
        {
            // Whitespace
            String host = "  somerandomhostname  ";
            int port = 99;
            assertThat(NetUtils.unresolvedHostAndPortToNormalizedString(host, port))
                    .isEqualTo(host.trim() + ":" + port);
        }
        {
            // Illegal hostnames
            String host = "illegalhost.";
            int port = 42;
            try {
                NetUtils.unresolvedHostAndPortToNormalizedString(host, port);
                fail("unknown failure");
            } catch (Exception ignored) {
            }
            // Illegal hostnames
            host = "illegalhost:fasf";
            try {
                NetUtils.unresolvedHostAndPortToNormalizedString(host, port);
                fail("unknown failure");
            } catch (Exception ignored) {
            }
        }
        {
            // Illegal port ranges
            String host = "1.2.3.4";
            int port = -1;
            try {
                NetUtils.unresolvedHostAndPortToNormalizedString(host, port);
                fail("unknown failure");
            } catch (Exception ignored) {
            }
        }
        {
            // lower case conversion of hostnames
            String host = "CamelCaseHostName";
            int port = 99;
            assertThat(NetUtils.unresolvedHostAndPortToNormalizedString(host, port))
                    .isEqualTo(host.toLowerCase() + ":" + port);
        }
    }
}
