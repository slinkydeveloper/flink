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

package org.apache.flink.yarn;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.security.contexts.HadoopSecurityContext;
import org.apache.flink.test.util.SecureTestEnvironment;
import org.apache.flink.test.util.TestingSecurityContext;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.util.TestHadoopModuleFactory;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An extension of the {@link YARNSessionFIFOITCase} that runs the tests in a secured YARN cluster.
 */
public class YARNSessionFIFOSecuredITCase extends YARNSessionFIFOITCase {

    protected static final Logger LOG = LoggerFactory.getLogger(YARNSessionFIFOSecuredITCase.class);

    @BeforeClass
    public static void setup() {

        LOG.info("starting secure cluster environment for testing");

        YARN_CONFIGURATION.setClass(
                YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class);
        YARN_CONFIGURATION.setInt(YarnConfiguration.NM_PMEM_MB, 768);
        YARN_CONFIGURATION.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 512);
        YARN_CONFIGURATION.set(YarnTestBase.TEST_CLUSTER_NAME_KEY, "flink-yarn-tests-fifo-secured");

        SecureTestEnvironment.prepare(tmp);

        populateYarnSecureConfigurations(
                YARN_CONFIGURATION,
                SecureTestEnvironment.getHadoopServicePrincipal(),
                SecureTestEnvironment.getTestKeytab());

        Configuration flinkConfig = new Configuration();
        flinkConfig.setString(
                SecurityOptions.KERBEROS_LOGIN_KEYTAB, SecureTestEnvironment.getTestKeytab());
        flinkConfig.setString(
                SecurityOptions.KERBEROS_LOGIN_PRINCIPAL,
                SecureTestEnvironment.getHadoopServicePrincipal());

        // Setting customized security module class.
        TestHadoopModuleFactory.hadoopConfiguration = YARN_CONFIGURATION;
        flinkConfig.set(
                SecurityOptions.SECURITY_MODULE_FACTORY_CLASSES,
                Collections.singletonList("org.apache.flink.yarn.util.TestHadoopModuleFactory"));
        flinkConfig.set(
                SecurityOptions.SECURITY_CONTEXT_FACTORY_CLASSES,
                Collections.singletonList(
                        "org.apache.flink.yarn.util.TestHadoopSecurityContextFactory"));

        SecurityConfiguration securityConfig = new SecurityConfiguration(flinkConfig);

        try {
            TestingSecurityContext.install(
                    securityConfig, SecureTestEnvironment.getClientSecurityConfigurationMap());

            // This is needed to ensure that SecurityUtils are run within a ugi.doAs section
            // Since we already logged in here in @BeforeClass, even a no-op security context will
            // still work.
            assertThat(SecurityUtils.getInstalledContext())
                    .as("HadoopSecurityContext must be installed")
                    .isInstanceOf(HadoopSecurityContext.class);
            SecurityUtils.getInstalledContext()
                    .runSecured(
                            new Callable<Object>() {
                                @Override
                                public Integer call() {
                                    startYARNSecureMode(
                                            YARN_CONFIGURATION,
                                            SecureTestEnvironment.getHadoopServicePrincipal(),
                                            SecureTestEnvironment.getTestKeytab());
                                    return null;
                                }
                            });

        } catch (Exception e) {
            throw new RuntimeException(
                    "Exception occurred while setting up secure test context. Reason: {}", e);
        }
    }

    @AfterClass
    public static void teardownSecureCluster() {
        LOG.info("tearing down secure cluster environment");
        SecureTestEnvironment.cleanup();
    }

    @Test(timeout = 60000) // timeout after a minute.
    public void testDetachedModeSecureWithPreInstallKeytab() throws Exception {
        runTest(
                () -> {
                    Map<String, String> securityProperties = new HashMap<>();
                    if (SecureTestEnvironment.getTestKeytab() != null) {
                        // client login keytab
                        securityProperties.put(
                                SecurityOptions.KERBEROS_LOGIN_KEYTAB.key(),
                                SecureTestEnvironment.getTestKeytab());
                        // pre-install Yarn local keytab, since both reuse the same temporary folder
                        // "tmp"
                        securityProperties.put(
                                YarnConfigOptions.LOCALIZED_KEYTAB_PATH.key(),
                                SecureTestEnvironment.getTestKeytab());
                        // unset keytab localization
                        securityProperties.put(YarnConfigOptions.SHIP_LOCAL_KEYTAB.key(), "false");
                    }
                    if (SecureTestEnvironment.getHadoopServicePrincipal() != null) {
                        securityProperties.put(
                                SecurityOptions.KERBEROS_LOGIN_PRINCIPAL.key(),
                                SecureTestEnvironment.getHadoopServicePrincipal());
                    }
                    final ApplicationId applicationId = runDetachedModeTest(securityProperties);
                    verifyResultContainsKerberosKeytab(applicationId);
                });
    }

    @Test(timeout = 60000) // timeout after a minute.
    @Override
    public void testDetachedMode() throws Exception {
        runTest(
                () -> {
                    Map<String, String> securityProperties = new HashMap<>();
                    if (SecureTestEnvironment.getTestKeytab() != null) {
                        securityProperties.put(
                                SecurityOptions.KERBEROS_LOGIN_KEYTAB.key(),
                                SecureTestEnvironment.getTestKeytab());
                    }
                    if (SecureTestEnvironment.getHadoopServicePrincipal() != null) {
                        securityProperties.put(
                                SecurityOptions.KERBEROS_LOGIN_PRINCIPAL.key(),
                                SecureTestEnvironment.getHadoopServicePrincipal());
                    }
                    final ApplicationId applicationId = runDetachedModeTest(securityProperties);
                    verifyResultContainsKerberosKeytab(applicationId);
                });
    }

    private static void verifyResultContainsKerberosKeytab(ApplicationId applicationId)
            throws Exception {
        final String[] mustHave = {"Login successful for user", "using keytab file"};
        final boolean jobManagerRunsWithKerberos =
                verifyStringsInNamedLogFiles(mustHave, applicationId, "jobmanager.log");
        final boolean taskManagerRunsWithKerberos =
                verifyStringsInNamedLogFiles(mustHave, applicationId, "taskmanager.log");

        assertThat(jobManagerRunsWithKerberos && taskManagerRunsWithKerberos)
                .as("The JobManager and the TaskManager should both run with Kerberos.")
                .isEqualTo(true);

        final List<String> amRMTokens =
                Lists.newArrayList(AMRMTokenIdentifier.KIND_NAME.toString());
        final String jobmanagerContainerId = getContainerIdByLogName("jobmanager.log");
        final String taskmanagerContainerId = getContainerIdByLogName("taskmanager.log");
        final boolean jobmanagerWithAmRmToken =
                verifyTokenKindInContainerCredentials(amRMTokens, jobmanagerContainerId);
        final boolean taskmanagerWithAmRmToken =
                verifyTokenKindInContainerCredentials(amRMTokens, taskmanagerContainerId);

        assertThat(jobmanagerWithAmRmToken)
                .as("The JobManager should have AMRMToken.")
                .isEqualTo(true);
        assertThat(taskmanagerWithAmRmToken)
                .as("The TaskManager should not have AMRMToken.")
                .isEqualTo(false);
    }

    /* For secure cluster testing, it is enough to run only one test and override below test methods
     * to keep the overall build time minimal
     */
    @Override
    public void testQueryCluster() {}
}
