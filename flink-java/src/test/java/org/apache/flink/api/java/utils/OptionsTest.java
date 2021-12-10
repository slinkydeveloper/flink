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

package org.apache.flink.api.java.utils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for the Options utility class. */
@Deprecated
public class OptionsTest {

    @Rule public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testChoicesWithInvalidDefaultValue() throws RequiredParametersException {
        expectedException.expect(RequiredParametersException.class);
        expectedException.expectMessage(
                "Default value d is not in the list of valid values for option choices");

        Option option = new Option("choices").choices("a", "b", "c");
        option.defaultValue("d");
    }

    @Test
    public void testChoicesWithValidDefaultValue() {
        Option option = null;
        try {
            option = new Option("choices").choices("a", "b", "c");
            option = option.defaultValue("a");
        } catch (RequiredParametersException e) {
            fail("Exception thrown: " + e.getMessage());
        }

        assertThat("a").isEqualTo(option.getDefaultValue());
    }

    @Test
    public void testChoicesWithInvalidDefautlValue() throws RequiredParametersException {
        expectedException.expect(RequiredParametersException.class);
        expectedException.expectMessage(
                "Valid values for option choices do not contain defined default value x");

        Option option = new Option("choices").defaultValue("x");
        option.choices("a", "b");
    }

    @Test
    public void testIsCastableToDefinedTypeWithDefaultType() {
        Option option = new Option("name");
        assertThat(option.isCastableToDefinedType("some value")).isTrue();
    }

    @Test
    public void testIsCastableToDefinedTypeWithMatchingTypes() {
        // Integer
        Option option = new Option("name").type(OptionType.INTEGER);
        assertThat(option.isCastableToDefinedType("15")).isTrue();

        // Double
        Option optionDouble = new Option("name").type(OptionType.DOUBLE);
        assertThat(optionDouble.isCastableToDefinedType("15.0")).isTrue();

        // Boolean
        Option optionFloat = new Option("name").type(OptionType.BOOLEAN);
        assertThat(optionFloat.isCastableToDefinedType("true")).isTrue();
    }

    @Test
    public void testIsCastableToDefinedTypeWithNonMatchingTypes() {
        // Integer
        Option option = new Option("name").type(OptionType.INTEGER);
        assertThat(option.isCastableToDefinedType("true")).isFalse();

        // Double
        Option optionDouble = new Option("name").type(OptionType.DOUBLE);
        assertThat(optionDouble.isCastableToDefinedType("name")).isFalse();

        // Boolean
        Option optionFloat = new Option("name").type(OptionType.BOOLEAN);
        assertThat(optionFloat.isCastableToDefinedType("15")).isFalse();
    }
}
