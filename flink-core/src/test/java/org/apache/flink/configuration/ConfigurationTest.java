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

package org.apache.flink.configuration;

import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * This class contains test for the configuration package. In particular, the serialization of
 * {@link Configuration} objects is tested.
 */
public class ConfigurationTest extends TestLogger {

    private static final ConfigOption<String> STRING_OPTION =
            ConfigOptions.key("test-string-key").noDefaultValue();

    private static final ConfigOption<List<String>> LIST_STRING_OPTION =
            ConfigOptions.key("test-list-key").stringType().asList().noDefaultValue();

    private static final ConfigOption<Map<String, String>> MAP_OPTION =
            ConfigOptions.key("test-map-key").mapType().noDefaultValue();

    private static final ConfigOption<Duration> DURATION_OPTION =
            ConfigOptions.key("test-duration-key").durationType().noDefaultValue();

    private static final Map<String, String> PROPERTIES_MAP = new HashMap<>();

    static {
        PROPERTIES_MAP.put("prop1", "value1");
        PROPERTIES_MAP.put("prop2", "12");
    }

    private static final String MAP_PROPERTY_1 = MAP_OPTION.key() + ".prop1";

    private static final String MAP_PROPERTY_2 = MAP_OPTION.key() + ".prop2";

    /** This test checks the serialization/deserialization of configuration objects. */
    @Test
    public void testConfigurationSerializationAndGetters() {
        try {
            final Configuration orig = new Configuration();
            orig.setString("mykey", "myvalue");
            orig.setInteger("mynumber", 100);
            orig.setLong("longvalue", 478236947162389746L);
            orig.setFloat("PI", 3.1415926f);
            orig.setDouble("E", Math.E);
            orig.setBoolean("shouldbetrue", true);
            orig.setBytes("bytes sequence", new byte[] {1, 2, 3, 4, 5});
            orig.setClass("myclass", this.getClass());

            final Configuration copy = InstantiationUtil.createCopyWritable(orig);
            assertThat(copy.getString("mykey", "null")).isEqualTo("myvalue");
            assertThat(copy.getInteger("mynumber", 0)).isEqualTo(100);
            assertThat(copy.getLong("longvalue", 0L)).isEqualTo(478236947162389746L);
            assertThat(copy.getFloat("PI", 3.1415926f)).isEqualTo(3.1415926f);
            assertThat(copy.getDouble("E", 0.0)).isEqualTo(Math.E);
            assertThat(copy.getBoolean("shouldbetrue", false)).isEqualTo(true);
            assertThat(copy.getBytes("bytes sequence", null)).isEqualTo(new byte[] {1, 2, 3, 4, 5});
            assertThat(copy.getClass("myclass", null, getClass().getClassLoader()))
                    .isEqualTo(getClass());

            assertThat(copy).isEqualTo(orig);
            assertThat(copy.keySet()).isEqualTo(orig.keySet());
            assertThat(copy.hashCode()).isEqualTo(orig.hashCode());

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCopyConstructor() {
        try {
            final String key = "theKey";

            Configuration cfg1 = new Configuration();
            cfg1.setString(key, "value");

            Configuration cfg2 = new Configuration(cfg1);
            cfg2.setString(key, "another value");

            assertThat(cfg1.getString(key, "")).isEqualTo("value");
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testOptionWithDefault() {
        Configuration cfg = new Configuration();
        cfg.setInteger("int-key", 11);
        cfg.setString("string-key", "abc");

        ConfigOption<String> presentStringOption =
                ConfigOptions.key("string-key").defaultValue("my-beautiful-default");
        ConfigOption<Integer> presentIntOption = ConfigOptions.key("int-key").defaultValue(87);

        assertThat(cfg.getString(presentStringOption)).isEqualTo("abc");
        assertThat(cfg.getValue(presentStringOption)).isEqualTo("abc");

        assertThat(cfg.getInteger(presentIntOption)).isEqualTo(11);
        assertThat(cfg.getValue(presentIntOption)).isEqualTo("11");

        // test getting default when no value is present

        ConfigOption<String> stringOption =
                ConfigOptions.key("test").defaultValue("my-beautiful-default");
        ConfigOption<Integer> intOption = ConfigOptions.key("test2").defaultValue(87);

        // getting strings with default value should work
        assertThat(cfg.getValue(stringOption)).isEqualTo("my-beautiful-default");
        assertThat(cfg.getString(stringOption)).isEqualTo("my-beautiful-default");

        // overriding the default should work
        assertThat(cfg.getString(stringOption, "override")).isEqualTo("override");

        // getting a primitive with a default value should work
        assertThat(cfg.getInteger(intOption)).isEqualTo(87);
        assertThat(cfg.getValue(intOption)).isEqualTo("87");
    }

    @Test
    public void testOptionWithNoDefault() {
        Configuration cfg = new Configuration();
        cfg.setInteger("int-key", 11);
        cfg.setString("string-key", "abc");

        ConfigOption<String> presentStringOption = ConfigOptions.key("string-key").noDefaultValue();

        assertThat(cfg.getString(presentStringOption)).isEqualTo("abc");
        assertThat(cfg.getValue(presentStringOption)).isEqualTo("abc");

        // test getting default when no value is present

        ConfigOption<String> stringOption = ConfigOptions.key("test").noDefaultValue();

        // getting strings for null should work
        assertThat(cfg.getValue(stringOption)).isNull();
        assertThat(cfg.getString(stringOption)).isNull();

        // overriding the null default should work
        assertThat(cfg.getString(stringOption, "override")).isEqualTo("override");
    }

    @Test
    public void testDeprecatedKeys() {
        Configuration cfg = new Configuration();
        cfg.setInteger("the-key", 11);
        cfg.setInteger("old-key", 12);
        cfg.setInteger("older-key", 13);

        ConfigOption<Integer> matchesFirst =
                ConfigOptions.key("the-key")
                        .defaultValue(-1)
                        .withDeprecatedKeys("old-key", "older-key");

        ConfigOption<Integer> matchesSecond =
                ConfigOptions.key("does-not-exist")
                        .defaultValue(-1)
                        .withDeprecatedKeys("old-key", "older-key");

        ConfigOption<Integer> matchesThird =
                ConfigOptions.key("does-not-exist")
                        .defaultValue(-1)
                        .withDeprecatedKeys("foo", "older-key");

        ConfigOption<Integer> notContained =
                ConfigOptions.key("does-not-exist")
                        .defaultValue(-1)
                        .withDeprecatedKeys("not-there", "also-not-there");

        assertThat(cfg.getInteger(matchesFirst)).isEqualTo(11);
        assertThat(cfg.getInteger(matchesSecond)).isEqualTo(12);
        assertThat(cfg.getInteger(matchesThird)).isEqualTo(13);
        assertThat(cfg.getInteger(notContained)).isEqualTo(-1);
    }

    @Test
    public void testFallbackKeys() {
        Configuration cfg = new Configuration();
        cfg.setInteger("the-key", 11);
        cfg.setInteger("old-key", 12);
        cfg.setInteger("older-key", 13);

        ConfigOption<Integer> matchesFirst =
                ConfigOptions.key("the-key")
                        .defaultValue(-1)
                        .withFallbackKeys("old-key", "older-key");

        ConfigOption<Integer> matchesSecond =
                ConfigOptions.key("does-not-exist")
                        .defaultValue(-1)
                        .withFallbackKeys("old-key", "older-key");

        ConfigOption<Integer> matchesThird =
                ConfigOptions.key("does-not-exist")
                        .defaultValue(-1)
                        .withFallbackKeys("foo", "older-key");

        ConfigOption<Integer> notContained =
                ConfigOptions.key("does-not-exist")
                        .defaultValue(-1)
                        .withFallbackKeys("not-there", "also-not-there");

        assertThat(cfg.getInteger(matchesFirst)).isEqualTo(11);
        assertThat(cfg.getInteger(matchesSecond)).isEqualTo(12);
        assertThat(cfg.getInteger(matchesThird)).isEqualTo(13);
        assertThat(cfg.getInteger(notContained)).isEqualTo(-1);
    }

    @Test
    public void testFallbackAndDeprecatedKeys() {
        final ConfigOption<Integer> fallback = ConfigOptions.key("fallback").defaultValue(-1);

        final ConfigOption<Integer> deprecated = ConfigOptions.key("deprecated").defaultValue(-1);

        final ConfigOption<Integer> mainOption =
                ConfigOptions.key("main")
                        .defaultValue(-1)
                        .withFallbackKeys(fallback.key())
                        .withDeprecatedKeys(deprecated.key());

        final Configuration fallbackCfg = new Configuration();
        fallbackCfg.setInteger(fallback, 1);
        assertThat(fallbackCfg.getInteger(mainOption)).isEqualTo(1);

        final Configuration deprecatedCfg = new Configuration();
        deprecatedCfg.setInteger(deprecated, 2);
        assertThat(deprecatedCfg.getInteger(mainOption)).isEqualTo(2);

        // reverse declaration of fallback and deprecated keys, fallback keys should always be used
        // first
        final ConfigOption<Integer> reversedMainOption =
                ConfigOptions.key("main")
                        .defaultValue(-1)
                        .withDeprecatedKeys(deprecated.key())
                        .withFallbackKeys(fallback.key());

        final Configuration deprecatedAndFallBackConfig = new Configuration();
        deprecatedAndFallBackConfig.setInteger(fallback, 1);
        deprecatedAndFallBackConfig.setInteger(deprecated, 2);
        assertThat(deprecatedAndFallBackConfig.getInteger(mainOption)).isEqualTo(1);
        assertThat(deprecatedAndFallBackConfig.getInteger(reversedMainOption)).isEqualTo(1);
    }

    @Test
    public void testRemove() {
        Configuration cfg = new Configuration();
        cfg.setInteger("a", 1);
        cfg.setInteger("b", 2);

        ConfigOption<Integer> validOption = ConfigOptions.key("a").defaultValue(-1);

        ConfigOption<Integer> deprecatedOption =
                ConfigOptions.key("c").defaultValue(-1).withDeprecatedKeys("d", "b");

        ConfigOption<Integer> unexistedOption =
                ConfigOptions.key("e").defaultValue(-1).withDeprecatedKeys("f", "g", "j");

        assertThat(2).as("Wrong expectation about size").isEqualTo(cfg.keySet().size());
        assertThat(cfg.removeConfig(validOption)).as("Expected 'validOption' is removed").isTrue();
        assertThat(1).as("Wrong expectation about size").isEqualTo(cfg.keySet().size());
        assertThat(cfg.removeConfig(deprecatedOption))
                .as("Expected 'existedOption' is removed")
                .isTrue();
        assertThat(0).as("Wrong expectation about size").isEqualTo(cfg.keySet().size());
        assertThat(cfg.removeConfig(unexistedOption))
                .as("Expected 'unexistedOption' is not removed")
                .isFalse();
    }

    @Test
    public void testShouldParseValidStringToEnum() {
        final Configuration configuration = new Configuration();
        configuration.setString(STRING_OPTION.key(), TestEnum.VALUE1.toString());

        final TestEnum parsedEnumValue = configuration.getEnum(TestEnum.class, STRING_OPTION);
        assertThat(parsedEnumValue).isEqualTo(TestEnum.VALUE1);
    }

    @Test
    public void testShouldParseValidStringToEnumIgnoringCase() {
        final Configuration configuration = new Configuration();
        configuration.setString(STRING_OPTION.key(), TestEnum.VALUE1.toString().toLowerCase());

        final TestEnum parsedEnumValue = configuration.getEnum(TestEnum.class, STRING_OPTION);
        assertThat(parsedEnumValue).isEqualTo(TestEnum.VALUE1);
    }

    @Test
    public void testThrowsExceptionIfTryingToParseInvalidStringForEnum() {
        final Configuration configuration = new Configuration();
        final String invalidValueForTestEnum = "InvalidValueForTestEnum";
        configuration.setString(STRING_OPTION.key(), invalidValueForTestEnum);

        try {
            configuration.getEnum(TestEnum.class, STRING_OPTION);
            fail("Expected exception not thrown");
        } catch (IllegalArgumentException e) {
            final String expectedMessage =
                    "Value for config option "
                            + STRING_OPTION.key()
                            + " must be one of [VALUE1, VALUE2] (was "
                            + invalidValueForTestEnum
                            + ")";
            assertThat(e.getMessage()).contains(expectedMessage);
        }
    }

    @Test
    public void testToMap() {
        final Configuration configuration = new Configuration();
        final String listValues = "value1;value2;value3";
        configuration.set(LIST_STRING_OPTION, Arrays.asList(listValues.split(";")));

        final String mapValues = "key1:value1,key2:value2";
        configuration.set(
                MAP_OPTION,
                Arrays.stream(mapValues.split(","))
                        .collect(Collectors.toMap(e -> e.split(":")[0], e -> e.split(":")[1])));

        final Duration duration = Duration.ofMillis(3000);
        configuration.set(DURATION_OPTION, duration);

        assertThat(configuration.toMap().get(LIST_STRING_OPTION.key())).isEqualTo(listValues);
        assertThat(configuration.toMap().get(MAP_OPTION.key())).isEqualTo(mapValues);
        assertThat(configuration.toMap().get(DURATION_OPTION.key())).isEqualTo("3 s");
    }

    @Test
    public void testMapNotContained() {
        final Configuration cfg = new Configuration();

        assertThat(cfg.getOptional(MAP_OPTION).isPresent()).isFalse();
        assertThat(cfg.contains(MAP_OPTION)).isFalse();
    }

    @Test
    public void testMapWithPrefix() {
        final Configuration cfg = new Configuration();
        cfg.setString(MAP_PROPERTY_1, "value1");
        cfg.setInteger(MAP_PROPERTY_2, 12);

        assertThat(PROPERTIES_MAP).isEqualTo(cfg.get(MAP_OPTION));
        assertThat(cfg.contains(MAP_OPTION)).isTrue();
    }

    @Test
    public void testMapWithoutPrefix() {
        final Configuration cfg = new Configuration();
        cfg.set(MAP_OPTION, PROPERTIES_MAP);

        assertThat(PROPERTIES_MAP).isEqualTo(cfg.get(MAP_OPTION));
        assertThat(cfg.contains(MAP_OPTION)).isTrue();
    }

    @Test
    public void testMapNonPrefixHasPrecedence() {
        final Configuration cfg = new Configuration();
        cfg.set(MAP_OPTION, PROPERTIES_MAP);
        cfg.setString(MAP_PROPERTY_1, "value1");
        cfg.setInteger(MAP_PROPERTY_2, 99999);

        assertThat(PROPERTIES_MAP).isEqualTo(cfg.get(MAP_OPTION));
        assertThat(cfg.contains(MAP_OPTION)).isTrue();
        assertThat(cfg.containsKey(MAP_PROPERTY_1)).isTrue();
    }

    @Test
    public void testMapThatOverwritesPrefix() {
        final Configuration cfg = new Configuration();
        cfg.setString(MAP_PROPERTY_1, "value1");
        cfg.setInteger(MAP_PROPERTY_2, 99999);
        cfg.set(MAP_OPTION, PROPERTIES_MAP);

        assertThat(PROPERTIES_MAP).isEqualTo(cfg.get(MAP_OPTION));
        assertThat(cfg.contains(MAP_OPTION)).isTrue();
        assertThat(cfg.containsKey(MAP_PROPERTY_1)).isFalse();
    }

    @Test
    public void testMapRemovePrefix() {
        final Configuration cfg = new Configuration();
        cfg.setString(MAP_PROPERTY_1, "value1");
        cfg.setInteger(MAP_PROPERTY_2, 99999);
        cfg.removeConfig(MAP_OPTION);

        assertThat(cfg.contains(MAP_OPTION)).isFalse();
        assertThat(cfg.containsKey(MAP_PROPERTY_1)).isFalse();
        assertThat(cfg.containsKey(MAP_PROPERTY_2)).isFalse();
    }

    // --------------------------------------------------------------------------------------------
    // Test classes
    // --------------------------------------------------------------------------------------------

    enum TestEnum {
        VALUE1,
        VALUE2
    }
}
