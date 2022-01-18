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

package org.apache.flink.test.util;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Like {@link MiniClusterWithClientExtension}, but tied to the context of the whole test class,
 * rather than the context of the single tests.
 */
public class SharedMiniClusterWithClientExtension extends MiniClusterWithClientExtension
        implements BeforeAllCallback, AfterAllCallback {
    public SharedMiniClusterWithClientExtension(
            final MiniClusterResourceConfiguration miniClusterResourceConfiguration) {
        super(miniClusterResourceConfiguration);
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        super.before(extensionContext);
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {
        super.after(extensionContext);
    }

    @Override
    public void before(ExtensionContext context) throws Exception {
        // No-op
    }

    @Override
    public void after(ExtensionContext context) throws Exception {
        // No-op
    }
}
