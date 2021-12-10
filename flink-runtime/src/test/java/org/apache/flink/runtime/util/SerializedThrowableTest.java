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

package org.apache.flink.runtime.util;

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.testutils.ClassLoaderUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.SerializedThrowable;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class SerializedThrowableTest {

    @Test
    public void testIdenticalMessageAndStack() {
        try {
            IllegalArgumentException original = new IllegalArgumentException("test message");
            SerializedThrowable serialized = new SerializedThrowable(original);

            assertThat(serialized.getMessage()).isEqualTo(original.getMessage());
            assertThat(serialized.toString()).isEqualTo(original.toString());

            assertThat(ExceptionUtils.stringifyException(serialized))
                    .isEqualTo(ExceptionUtils.stringifyException(original));

            assertThat(serialized.getStackTrace()).isEqualTo(original.getStackTrace());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testSerialization() {
        try {
            // We need an exception whose class is not in the core class loader
            final ClassLoaderUtils.ObjectAndClassLoader<Exception> outsideClassLoading =
                    ClassLoaderUtils.createExceptionObjectFromNewClassLoader();
            ClassLoader loader = outsideClassLoading.getClassLoader();
            Exception userException = outsideClassLoading.getObject();
            Class<?> clazz = userException.getClass();

            // check that we cannot simply copy the exception
            try {
                byte[] serialized = InstantiationUtil.serializeObject(userException);
                InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());
                fail("should fail with a class not found exception");
            } catch (ClassNotFoundException e) {
                // as we want it
            }

            // validate that the SerializedThrowable mimics the original exception
            SerializedThrowable serialized = new SerializedThrowable(userException);
            assertThat(serialized.getMessage()).isEqualTo(userException.getMessage());
            assertThat(serialized.toString()).isEqualTo(userException.toString());
            assertThat(ExceptionUtils.stringifyException(serialized))
                    .isEqualTo(ExceptionUtils.stringifyException(userException));
            assertThat(serialized.getStackTrace()).isEqualTo(userException.getStackTrace());

            // copy the serialized throwable and make sure everything still works
            SerializedThrowable copy = CommonTestUtils.createCopySerializable(serialized);
            assertThat(copy.getMessage()).isEqualTo(userException.getMessage());
            assertThat(copy.toString()).isEqualTo(userException.toString());
            assertThat(ExceptionUtils.stringifyException(copy))
                    .isEqualTo(ExceptionUtils.stringifyException(userException));
            assertThat(copy.getStackTrace()).isEqualTo(userException.getStackTrace());

            // deserialize the proper exception
            Throwable deserialized = copy.deserializeError(loader);
            assertThat(deserialized.getClass()).isEqualTo(clazz);

            // deserialization with the wrong classloader does not lead to a failure
            Throwable wronglyDeserialized = copy.deserializeError(getClass().getClassLoader());
            assertThat(ExceptionUtils.stringifyException(wronglyDeserialized))
                    .isEqualTo(ExceptionUtils.stringifyException(userException));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCauseChaining() {
        Exception cause2 = new Exception("level2");
        Exception cause1 = new Exception("level1", cause2);
        Exception root = new Exception("level0", cause1);

        SerializedThrowable st = new SerializedThrowable(root);

        assertThat(st.getMessage()).isEqualTo("level0");

        assertThat(st.getCause()).isNotNull();
        assertThat(st.getCause().getMessage()).isEqualTo("level1");

        assertThat(st.getCause().getCause()).isNotNull();
        assertThat(st.getCause().getCause().getMessage()).isEqualTo("level2");
    }

    @Test
    public void testCyclicCauseChaining() {
        Exception cause3 = new Exception("level3");
        Exception cause2 = new Exception("level2", cause3);
        Exception cause1 = new Exception("level1", cause2);
        Exception root = new Exception("level0", cause1);

        // introduce a cyclic reference
        cause3.initCause(cause1);

        SerializedThrowable st = new SerializedThrowable(root);

        assertThat(st.getStackTrace()).isEqualTo(root.getStackTrace());
        assertThat(ExceptionUtils.stringifyException(st))
                .isEqualTo(ExceptionUtils.stringifyException(root));
    }

    @Test
    public void testCopyPreservesCause() {
        Exception original = new Exception("original message");
        Exception parent = new Exception("parent message", original);

        SerializedThrowable serialized = new SerializedThrowable(parent);
        assertThat(serialized.getCause()).isNotNull();

        SerializedThrowable copy = new SerializedThrowable(serialized);
        assertThat(copy.getMessage()).isEqualTo("parent message");
        assertThat(copy.getCause()).isNotNull();
        assertThat(copy.getCause().getMessage()).isEqualTo("original message");
    }
}
