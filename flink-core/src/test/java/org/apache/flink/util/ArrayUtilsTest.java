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

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link ArrayUtils}. */
public class ArrayUtilsTest extends TestLogger {

    @Test
    public void concatWithEmptyArray() {
        String[] emptyArray = new String[] {};
        String[] nonEmptyArray = new String[] {"some value"};

        assertThat(ArrayUtils.concat(emptyArray, nonEmptyArray))
                .as("Should return the non empty array")
                .isSameAs(nonEmptyArray);

        assertThat(ArrayUtils.concat(nonEmptyArray, emptyArray))
                .as("Should return the non empty array")
                .isSameAs(nonEmptyArray);
    }

    @Test
    public void concatArrays() {
        String[] array1 = new String[] {"A", "B", "C", "D", "E", "F", "G"};
        String[] array2 = new String[] {"1", "2", "3"};

        assertThat(ArrayUtils.concat(array1, array2))
                .isEqualTo(new String[] {"A", "B", "C", "D", "E", "F", "G", "1", "2", "3"});

        assertThat(ArrayUtils.concat(array2, array1))
                .isEqualTo(new String[] {"1", "2", "3", "A", "B", "C", "D", "E", "F", "G"});
    }
}
