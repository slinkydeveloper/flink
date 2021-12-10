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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link IOUtils}. */
public class IOUtilsTest extends TestLogger {

    @Test
    public void testTryReadFullyFromLongerStream() throws IOException {
        ByteArrayInputStream inputStream = new ByteArrayInputStream("test-data".getBytes());

        byte[] out = new byte[4];
        int read = IOUtils.tryReadFully(inputStream, out);

        assertThat(Arrays.copyOfRange(out, 0, read)).isEqualTo("test".getBytes());
    }

    @Test
    public void testTryReadFullyFromShorterStream() throws IOException {
        ByteArrayInputStream inputStream = new ByteArrayInputStream("t".getBytes());

        byte[] out = new byte[4];
        int read = IOUtils.tryReadFully(inputStream, out);

        assertThat(Arrays.copyOfRange(out, 0, read)).isEqualTo("t".getBytes());
    }
}
