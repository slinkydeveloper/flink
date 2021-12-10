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

package org.apache.flink.types.parser;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** */
public abstract class ParserTestBase<T> extends TestLogger {

    public abstract String[] getValidTestValues();

    public abstract T[] getValidTestResults();

    public abstract String[] getInvalidTestValues();

    public abstract boolean allowsEmptyField();

    public abstract FieldParser<T> getParser();

    public abstract Class<T> getTypeClass();

    @Test
    public void testTest() {
        assertThat(getParser()).isNotNull();
        assertThat(getTypeClass()).isNotNull();
        assertThat(getValidTestValues()).isNotNull();
        assertThat(getValidTestResults()).isNotNull();
        assertThat(getInvalidTestValues()).isNotNull();
        assertThat(getValidTestValues().length == getValidTestResults().length).isTrue();
    }

    @Test
    public void testGetValue() {
        try {
            FieldParser<?> parser = getParser();
            Object created = parser.createValue();

            assertThat(created).as("Null type created").isNotNull();
            assertThat(getTypeClass().isAssignableFrom(created.getClass()))
                    .as("Wrong type created")
                    .isTrue();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Test erroneous: " + e.getMessage());
        }
    }

    @Test
    public void testValidStringInIsolation() {
        try {
            String[] testValues = getValidTestValues();
            T[] results = getValidTestResults();

            for (int i = 0; i < testValues.length; i++) {

                FieldParser<T> parser1 = getParser();
                FieldParser<T> parser2 = getParser();
                FieldParser<T> parser3 = getParser();

                byte[] bytes1 = testValues[i].getBytes(ConfigConstants.DEFAULT_CHARSET);
                byte[] bytes2 = testValues[i].getBytes(ConfigConstants.DEFAULT_CHARSET);
                byte[] bytes3 = testValues[i].getBytes(ConfigConstants.DEFAULT_CHARSET);

                int numRead1 =
                        parser1.parseField(
                                bytes1, 0, bytes1.length, new byte[] {'|'}, parser1.createValue());
                int numRead2 =
                        parser2.parseField(
                                bytes2,
                                0,
                                bytes2.length,
                                new byte[] {'&', '&'},
                                parser2.createValue());
                int numRead3 =
                        parser3.parseField(
                                bytes3,
                                0,
                                bytes3.length,
                                new byte[] {'9', '9', '9'},
                                parser3.createValue());

                assertThat(numRead1 != -1)
                        .as("Parser declared the valid value " + testValues[i] + " as invalid.")
                        .isTrue();
                assertThat(numRead2 != -1)
                        .as("Parser declared the valid value " + testValues[i] + " as invalid.")
                        .isTrue();
                assertThat(numRead3 != -1)
                        .as("Parser declared the valid value " + testValues[i] + " as invalid.")
                        .isTrue();

                assertThat(numRead1)
                        .as("Invalid number of bytes read returned.")
                        .isEqualTo(bytes1.length);
                assertThat(numRead2)
                        .as("Invalid number of bytes read returned.")
                        .isEqualTo(bytes2.length);
                assertThat(numRead3)
                        .as("Invalid number of bytes read returned.")
                        .isEqualTo(bytes3.length);

                T result1 = parser1.getLastResult();
                T result2 = parser2.getLastResult();
                T result3 = parser3.getLastResult();

                assertThat(result1)
                        .as("Parser parsed wrong. " + testValues[i])
                        .isEqualTo(results[i]);
                assertThat(result2)
                        .as("Parser parsed wrong. " + testValues[i])
                        .isEqualTo(results[i]);
                assertThat(result3)
                        .as("Parser parsed wrong. " + testValues[i])
                        .isEqualTo(results[i]);
            }

        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Test erroneous: " + e.getMessage());
        }
    }

    @Test
    public void testValidStringInIsolationWithEndDelimiter() {
        try {
            String[] testValues = getValidTestValues();
            T[] results = getValidTestResults();

            for (int i = 0; i < testValues.length; i++) {

                FieldParser<T> parser1 = getParser();
                FieldParser<T> parser2 = getParser();

                String testVal1 = testValues[i] + "|";
                String testVal2 = testValues[i] + "&&&&";

                byte[] bytes1 = testVal1.getBytes(ConfigConstants.DEFAULT_CHARSET);
                byte[] bytes2 = testVal2.getBytes(ConfigConstants.DEFAULT_CHARSET);

                int numRead1 =
                        parser1.parseField(
                                bytes1, 0, bytes1.length, new byte[] {'|'}, parser1.createValue());
                int numRead2 =
                        parser2.parseField(
                                bytes2,
                                0,
                                bytes2.length,
                                new byte[] {'&', '&', '&', '&'},
                                parser2.createValue());

                assertThat(numRead1 != -1)
                        .as("Parser declared the valid value " + testValues[i] + " as invalid.")
                        .isTrue();
                assertThat(numRead2 != -1)
                        .as("Parser declared the valid value " + testValues[i] + " as invalid.")
                        .isTrue();

                assertThat(numRead1)
                        .as("Invalid number of bytes read returned.")
                        .isEqualTo(bytes1.length);
                assertThat(numRead2)
                        .as("Invalid number of bytes read returned.")
                        .isEqualTo(bytes2.length);

                T result1 = parser1.getLastResult();
                T result2 = parser2.getLastResult();

                assertThat(result1).as("Parser parsed wrong.").isEqualTo(results[i]);
                assertThat(result2).as("Parser parsed wrong.").isEqualTo(results[i]);
            }

        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Test erroneous: " + e.getMessage());
        }
    }

    @Test
    public void testConcatenated() {
        try {
            String[] testValues = getValidTestValues();
            T[] results = getValidTestResults();
            byte[] allBytesWithDelimiter = concatenate(testValues, new char[] {'|'}, true);
            byte[] allBytesNoDelimiterEnd = concatenate(testValues, new char[] {','}, false);

            FieldParser<T> parser1 = getParser();
            FieldParser<T> parser2 = getParser();

            T val1 = parser1.createValue();
            T val2 = parser2.createValue();

            int pos1 = 0;
            int pos2 = 0;

            for (int i = 0; i < results.length; i++) {
                pos1 =
                        parser1.parseField(
                                allBytesWithDelimiter,
                                pos1,
                                allBytesWithDelimiter.length,
                                new byte[] {'|'},
                                val1);
                pos2 =
                        parser2.parseField(
                                allBytesNoDelimiterEnd,
                                pos2,
                                allBytesNoDelimiterEnd.length,
                                new byte[] {','},
                                val2);

                assertThat(pos1 != -1)
                        .as("Parser declared the valid value " + testValues[i] + " as invalid.")
                        .isTrue();
                assertThat(pos2 != -1)
                        .as("Parser declared the valid value " + testValues[i] + " as invalid.")
                        .isTrue();

                T result1 = parser1.getLastResult();
                T result2 = parser2.getLastResult();

                assertThat(result1).as("Parser parsed wrong.").isEqualTo(results[i]);
                assertThat(result2).as("Parser parsed wrong.").isEqualTo(results[i]);
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Test erroneous: " + e.getMessage());
        }
    }

    @Test
    public void testConcatenatedMultiCharDelimiter() {
        try {
            String[] testValues = getValidTestValues();
            T[] results = getValidTestResults();
            byte[] allBytesWithDelimiter =
                    concatenate(testValues, new char[] {'&', '&', '&', '&'}, true);
            byte[] allBytesNoDelimiterEnd =
                    concatenate(testValues, new char[] {'9', '9', '9'}, false);

            FieldParser<T> parser1 = getParser();
            FieldParser<T> parser2 = getParser();

            T val1 = parser1.createValue();
            T val2 = parser2.createValue();

            int pos1 = 0;
            int pos2 = 0;

            for (int i = 0; i < results.length; i++) {
                pos1 =
                        parser1.parseField(
                                allBytesWithDelimiter,
                                pos1,
                                allBytesWithDelimiter.length,
                                new byte[] {'&', '&', '&', '&'},
                                val1);
                assertThat(pos1 != -1)
                        .as("Parser declared the valid value " + testValues[i] + " as invalid.")
                        .isTrue();
                T result1 = parser1.getLastResult();
                assertThat(result1).as("Parser parsed wrong.").isEqualTo(results[i]);

                pos2 =
                        parser2.parseField(
                                allBytesNoDelimiterEnd,
                                pos2,
                                allBytesNoDelimiterEnd.length,
                                new byte[] {'9', '9', '9'},
                                val2);
                assertThat(pos2 != -1)
                        .as("Parser declared the valid value " + testValues[i] + " as invalid.")
                        .isTrue();
                T result2 = parser2.getLastResult();
                assertThat(result2).as("Parser parsed wrong.").isEqualTo(results[i]);
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Test erroneous: " + e.getMessage());
        }
    }

    @Test
    public void testInValidStringInIsolation() {
        try {
            String[] testValues = getInvalidTestValues();

            for (int i = 0; i < testValues.length; i++) {

                FieldParser<T> parser = getParser();

                byte[] bytes = testValues[i].getBytes(ConfigConstants.DEFAULT_CHARSET);
                int numRead =
                        parser.parseField(
                                bytes, 0, bytes.length, new byte[] {'|'}, parser.createValue());

                assertThat(numRead == -1)
                        .as("Parser accepted the invalid value " + testValues[i] + ".")
                        .isTrue();
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Test erroneous: " + e.getMessage());
        }
    }

    @Test
    public void testInValidStringsMixedIn() {
        try {
            String[] validValues = getValidTestValues();
            T[] validResults = getValidTestResults();

            String[] invalidTestValues = getInvalidTestValues();

            FieldParser<T> parser = getParser();
            T value = parser.createValue();

            for (String invalid : invalidTestValues) {

                // place an invalid string in the middle
                String[] testLine = new String[validValues.length + 1];
                int splitPoint = validValues.length / 2;
                System.arraycopy(validValues, 0, testLine, 0, splitPoint);
                testLine[splitPoint] = invalid;
                System.arraycopy(
                        validValues,
                        splitPoint,
                        testLine,
                        splitPoint + 1,
                        validValues.length - splitPoint);

                byte[] bytes = concatenate(testLine, new char[] {'%'}, true);

                // read the valid parts
                int pos = 0;
                for (int i = 0; i < splitPoint; i++) {
                    pos = parser.parseField(bytes, pos, bytes.length, new byte[] {'%'}, value);

                    assertThat(pos != -1)
                            .as(
                                    "Parser declared the valid value "
                                            + validValues[i]
                                            + " as invalid.")
                            .isTrue();
                    T result = parser.getLastResult();
                    assertThat(result).as("Parser parsed wrong.").isEqualTo(validResults[i]);
                }

                // fail on the invalid part
                pos = parser.parseField(bytes, pos, bytes.length, new byte[] {'%'}, value);
                assertThat(pos == -1)
                        .as("Parser accepted the invalid value " + invalid + ".")
                        .isTrue();
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Test erroneous: " + e.getMessage());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testStaticParseMethod() {
        try {
            Method parseMethod = null;
            try {
                parseMethod =
                        getParser()
                                .getClass()
                                .getMethod(
                                        "parseField",
                                        byte[].class,
                                        int.class,
                                        int.class,
                                        char.class);
            } catch (NoSuchMethodException e) {
                return;
            }

            String[] testValues = getValidTestValues();
            T[] results = getValidTestResults();

            for (int i = 0; i < testValues.length; i++) {

                byte[] bytes = testValues[i].getBytes(ConfigConstants.DEFAULT_CHARSET);

                T result;
                try {
                    result = (T) parseMethod.invoke(null, bytes, 0, bytes.length, '|');
                } catch (InvocationTargetException e) {
                    e.getTargetException().printStackTrace();
                    fail("Error while parsing: " + e.getTargetException().getMessage());
                    return;
                }
                assertThat(result).as("Parser parsed wrong.").isEqualTo(results[i]);
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Test erroneous: " + e.getMessage());
        }
    }

    @Test
    public void testStaticParseMethodWithInvalidValues() {
        try {
            Method parseMethod = null;
            try {
                parseMethod =
                        getParser()
                                .getClass()
                                .getMethod(
                                        "parseField",
                                        byte[].class,
                                        int.class,
                                        int.class,
                                        char.class);
            } catch (NoSuchMethodException e) {
                return;
            }

            String[] testValues = getInvalidTestValues();

            for (int i = 0; i < testValues.length; i++) {

                byte[] bytes = testValues[i].getBytes(ConfigConstants.DEFAULT_CHARSET);

                try {
                    parseMethod.invoke(null, bytes, 0, bytes.length, '|');
                    fail("Static parse method accepted invalid value");
                } catch (InvocationTargetException e) {
                    // that's it!
                }
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Test erroneous: " + e.getMessage());
        }
    }

    private static byte[] concatenate(String[] values, char[] delimiter, boolean delimiterAtEnd) {
        int len = 0;
        for (String s : values) {
            len += s.length() + delimiter.length;
        }

        if (!delimiterAtEnd) {
            len -= delimiter.length;
        }

        int currPos = 0;
        byte[] result = new byte[len];

        for (int i = 0; i < values.length; i++) {
            String s = values[i];

            byte[] bytes = s.getBytes(ConfigConstants.DEFAULT_CHARSET);
            int numBytes = bytes.length;
            System.arraycopy(bytes, 0, result, currPos, numBytes);
            currPos += numBytes;

            if (delimiterAtEnd || i < values.length - 1) {
                for (int j = 0; j < delimiter.length; j++) result[currPos++] = (byte) delimiter[j];
            }
        }

        return result;
    }

    @Test
    public void testTrailingEmptyField() {
        try {
            FieldParser<T> parser = getParser();

            byte[] bytes = "||".getBytes(ConfigConstants.DEFAULT_CHARSET);

            for (int i = 0; i < 2; i++) {

                // test empty field with trailing delimiter when i = 0,
                // test empty field with out trailing delimiter when i= 1.
                int numRead =
                        parser.parseField(
                                bytes, i, bytes.length, new byte[] {'|'}, parser.createValue());

                assertThat(parser.getErrorState())
                        .isEqualTo(FieldParser.ParseErrorState.EMPTY_COLUMN);

                if (this.allowsEmptyField()) {
                    assertThat(numRead != -1)
                            .as("Parser declared the empty string as invalid.")
                            .isTrue();
                    assertThat(numRead)
                            .as("Invalid number of bytes read returned.")
                            .isEqualTo(i + 1);
                } else {
                    assertThat(numRead == -1).as("Parser accepted the empty string.").isTrue();
                }

                parser.resetParserState();
            }

        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail("Test erroneous: " + e.getMessage());
        }
    }
}
