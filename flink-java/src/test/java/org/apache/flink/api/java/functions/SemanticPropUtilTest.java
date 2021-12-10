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

package org.apache.flink.api.java.functions;

import org.apache.flink.api.common.operators.DualInputSemanticProperties;
import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.api.common.operators.SemanticProperties.InvalidSemanticAnnotationException;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for semantic properties utils. */
public class SemanticPropUtilTest {

    private final TypeInformation<?> threeIntTupleType =
            new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO);

    private final TypeInformation<?> fourIntTupleType =
            new TupleTypeInfo<Tuple4<Integer, Integer, Integer, Integer>>(
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO);

    private final TypeInformation<?> fiveIntTupleType =
            new TupleTypeInfo<Tuple5<Integer, Integer, Integer, Integer, Integer>>(
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.INT_TYPE_INFO);

    private final TypeInformation<?> threeMixedTupleType =
            new TupleTypeInfo<Tuple3<Integer, Long, String>>(
                    BasicTypeInfo.INT_TYPE_INFO,
                    BasicTypeInfo.LONG_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO);

    private final TypeInformation<?> nestedTupleType =
            new TupleTypeInfo<Tuple3<Tuple3<Integer, Integer, Integer>, Integer, Integer>>(
                    threeIntTupleType, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);

    private final TypeInformation<?> deepNestedTupleType =
            new TupleTypeInfo<
                    Tuple3<
                            Integer,
                            Tuple3<Tuple3<Integer, Integer, Integer>, Integer, Integer>,
                            Integer>>(
                    BasicTypeInfo.INT_TYPE_INFO, nestedTupleType, BasicTypeInfo.INT_TYPE_INFO);

    private final TypeInformation<?> pojoType = TypeExtractor.getForClass(TestPojo.class);

    private final TypeInformation<?> pojo2Type = TypeExtractor.getForClass(TestPojo2.class);

    private final TypeInformation<?> nestedPojoType =
            TypeExtractor.getForClass(NestedTestPojo.class);

    private final TypeInformation<?> pojoInTupleType =
            new TupleTypeInfo<Tuple3<Integer, Integer, TestPojo>>(
                    BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, pojoType);

    private final TypeInformation<?> intType = BasicTypeInfo.INT_TYPE_INFO;

    // --------------------------------------------------------------------------------------------
    // Projection Operator Properties
    // --------------------------------------------------------------------------------------------

    @Test
    public void testSingleProjectionProperties() {

        int[] pMap = new int[] {3, 0, 4};
        SingleInputSemanticProperties sp =
                SemanticPropUtil.createProjectionPropertiesSingle(
                        pMap, (CompositeType<?>) fiveIntTupleType);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 4).contains(2)).isTrue();

        pMap = new int[] {2, 2, 1, 1};
        sp =
                SemanticPropUtil.createProjectionPropertiesSingle(
                        pMap, (CompositeType<?>) fiveIntTupleType);
        assertThat(sp.getForwardingTargetFields(0, 1).size() == 2).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).size() == 2).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(1)).isTrue();

        pMap = new int[] {2, 0};
        sp =
                SemanticPropUtil.createProjectionPropertiesSingle(
                        pMap, (CompositeType<?>) nestedTupleType);
        assertThat(sp.getForwardingTargetFields(0, 4).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 0).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(3)).isTrue();

        pMap = new int[] {2, 0, 1};
        sp =
                SemanticPropUtil.createProjectionPropertiesSingle(
                        pMap, (CompositeType<?>) deepNestedTupleType);
        assertThat(sp.getForwardingTargetFields(0, 6).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 0).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 4).contains(5)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 5).contains(6)).isTrue();

        pMap = new int[] {2, 1};
        sp =
                SemanticPropUtil.createProjectionPropertiesSingle(
                        pMap, (CompositeType<?>) pojoInTupleType);
        assertThat(sp.getForwardingTargetFields(0, 2).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 4).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 5).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(4)).isTrue();
    }

    @Test
    public void testDualProjectionProperties() {

        int[] pMap = new int[] {4, 2, 0, 1, 3, 4};
        boolean[] iMap = new boolean[] {true, true, false, true, false, false};
        DualInputSemanticProperties sp =
                SemanticPropUtil.createProjectionPropertiesDual(
                        pMap, iMap, fiveIntTupleType, fiveIntTupleType);
        assertThat(sp.getForwardingTargetFields(0, 4).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 0).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 3).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 4).contains(5)).isTrue();

        pMap = new int[] {4, 2, 0, 4, 0, 1};
        iMap = new boolean[] {true, true, false, true, false, false};
        sp =
                SemanticPropUtil.createProjectionPropertiesDual(
                        pMap, iMap, fiveIntTupleType, fiveIntTupleType);
        assertThat(sp.getForwardingTargetFields(0, 4).size() == 2).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 0).size() == 2).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 4).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 0).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 4).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 0).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 1).contains(5)).isTrue();

        pMap = new int[] {2, 1, 0, 1};
        iMap = new boolean[] {false, false, true, true};
        sp =
                SemanticPropUtil.createProjectionPropertiesDual(
                        pMap, iMap, nestedTupleType, threeIntTupleType);
        assertThat(sp.getForwardingTargetFields(1, 2).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 1).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 0).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(5)).isTrue();

        pMap = new int[] {1, 0, 0};
        iMap = new boolean[] {false, false, true};
        sp =
                SemanticPropUtil.createProjectionPropertiesDual(
                        pMap, iMap, nestedTupleType, deepNestedTupleType);
        assertThat(sp.getForwardingTargetFields(1, 1).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 2).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 3).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 4).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 5).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 0).contains(5)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 0).contains(6)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(7)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(8)).isTrue();

        pMap = new int[] {4, 2, 1, 0};
        iMap = new boolean[] {true, false, true, false};
        sp =
                SemanticPropUtil.createProjectionPropertiesDual(
                        pMap, iMap, fiveIntTupleType, pojoInTupleType);
        assertThat(sp.getForwardingTargetFields(0, 4).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 2).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 3).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 4).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 5).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(5)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 0).contains(6)).isTrue();

        pMap = new int[] {2, 3, -1, 0};
        iMap = new boolean[] {true, true, false, true};
        sp = SemanticPropUtil.createProjectionPropertiesDual(pMap, iMap, fiveIntTupleType, intType);
        assertThat(sp.getForwardingTargetFields(0, 2).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 0).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 0).contains(3)).isTrue();

        pMap = new int[] {-1, -1};
        iMap = new boolean[] {false, true};
        sp = SemanticPropUtil.createProjectionPropertiesDual(pMap, iMap, intType, nestedPojoType);
        assertThat(sp.getForwardingTargetFields(1, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 1).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 2).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 3).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 4).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 5).contains(5)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 0).contains(6)).isTrue();

        pMap = new int[] {-1, -1};
        iMap = new boolean[] {true, false};
        sp = SemanticPropUtil.createProjectionPropertiesDual(pMap, iMap, intType, nestedPojoType);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 0).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 1).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 2).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 3).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 4).contains(5)).isTrue();
        assertThat(sp.getForwardingTargetFields(1, 5).contains(6)).isTrue();
    }

    // --------------------------------------------------------------------------------------------
    // Offset
    // --------------------------------------------------------------------------------------------

    @Test
    public void testAddSourceFieldOffset() {

        SingleInputSemanticProperties semProps = new SingleInputSemanticProperties();
        semProps.addForwardedField(0, 1);
        semProps.addForwardedField(0, 4);
        semProps.addForwardedField(2, 0);
        semProps.addForwardedField(4, 3);
        semProps.addReadFields(new FieldSet(0, 3));

        SemanticProperties offsetProps = SemanticPropUtil.addSourceFieldOffset(semProps, 5, 0);

        assertThat(offsetProps.getForwardingTargetFields(0, 0).size() == 2).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 0).contains(1)).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 0).contains(4)).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 1).size() == 0).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 2).size() == 1).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 2).contains(0)).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 3).size() == 0).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 4).size() == 1).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 4).contains(3)).isTrue();

        assertThat(offsetProps.getReadFields(0).size() == 2).isTrue();
        assertThat(offsetProps.getReadFields(0).contains(0)).isTrue();
        assertThat(offsetProps.getReadFields(0).contains(3)).isTrue();

        offsetProps = SemanticPropUtil.addSourceFieldOffset(semProps, 5, 3);

        assertThat(offsetProps.getForwardingTargetFields(0, 0).size() == 0).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 1).size() == 0).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 2).size() == 0).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 3).size() == 2).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 3).contains(1)).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 3).contains(4)).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 4).size() == 0).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 5).size() == 1).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 5).contains(0)).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 6).size() == 0).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 7).size() == 1).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 7).contains(3)).isTrue();

        assertThat(offsetProps.getReadFields(0).size() == 2).isTrue();
        assertThat(offsetProps.getReadFields(0).contains(3)).isTrue();
        assertThat(offsetProps.getReadFields(0).contains(6)).isTrue();

        semProps = new SingleInputSemanticProperties();
        SemanticPropUtil.addSourceFieldOffset(semProps, 1, 0);

        semProps = new SingleInputSemanticProperties();
        semProps.addForwardedField(0, 0);
        semProps.addForwardedField(1, 2);
        semProps.addForwardedField(2, 4);

        offsetProps = SemanticPropUtil.addSourceFieldOffset(semProps, 3, 2);

        assertThat(offsetProps.getForwardingTargetFields(0, 0).size() == 0).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 1).size() == 0).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 2).size() == 1).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 2).contains(0)).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 3).size() == 1).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 3).contains(2)).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 4).size() == 1).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 4).contains(4)).isTrue();
    }

    @Test
    public void testAddSourceFieldOffsets() {

        DualInputSemanticProperties semProps = new DualInputSemanticProperties();
        semProps.addForwardedField(0, 0, 1);
        semProps.addForwardedField(0, 3, 3);
        semProps.addForwardedField(1, 1, 2);
        semProps.addForwardedField(1, 1, 4);
        semProps.addReadFields(0, new FieldSet(1, 2));
        semProps.addReadFields(1, new FieldSet(0, 3, 4));

        DualInputSemanticProperties offsetProps =
                SemanticPropUtil.addSourceFieldOffsets(semProps, 4, 3, 1, 2);

        assertThat(offsetProps.getForwardingTargetFields(0, 0).size() == 0).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 1).size() == 1).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 1).contains(1)).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 2).size() == 0).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 3).size() == 0).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 4).size() == 1).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(0, 4).contains(3)).isTrue();

        assertThat(offsetProps.getForwardingTargetFields(1, 0).size() == 0).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(1, 1).size() == 0).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(1, 2).size() == 0).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(1, 3).size() == 2).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(1, 3).contains(2)).isTrue();
        assertThat(offsetProps.getForwardingTargetFields(1, 3).contains(4)).isTrue();

        assertThat(offsetProps.getReadFields(0).size() == 2).isTrue();
        assertThat(offsetProps.getReadFields(0).contains(2)).isTrue();
        assertThat(offsetProps.getReadFields(0).contains(3)).isTrue();
        assertThat(offsetProps.getReadFields(1).size() == 3).isTrue();
        assertThat(offsetProps.getReadFields(1).contains(2)).isTrue();
        assertThat(offsetProps.getReadFields(1).contains(5)).isTrue();
        assertThat(offsetProps.getReadFields(1).contains(6)).isTrue();

        semProps = new DualInputSemanticProperties();
        SemanticPropUtil.addSourceFieldOffsets(semProps, 4, 3, 2, 2);
    }

    // --------------------------------------------------------------------------------------------
    // Forwarded Fields Annotation
    // --------------------------------------------------------------------------------------------

    @Test
    public void testForwardedNoArrowIndividualStrings() {
        String[] forwardedFields = {"f2", "f3", "f0"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, fiveIntTupleType, fiveIntTupleType);

        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(3)).isTrue();
    }

    @Test
    public void testForwardedNoArrowOneString() {
        String[] forwardedFields = {"f2;f3;f0"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, fiveIntTupleType, fiveIntTupleType);

        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(3)).isTrue();

        forwardedFields[0] = "2;3;0";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, fiveIntTupleType, fiveIntTupleType);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(3)).isTrue();

        forwardedFields[0] = "2;3;0;";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, fiveIntTupleType, fiveIntTupleType);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(3)).isTrue();
    }

    @Test
    public void testForwardedNoArrowSpaces() {
        String[] forwardedFields = {"  f2  ;   f3  ;  f0   "};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, fiveIntTupleType, fiveIntTupleType);

        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(3)).isTrue();
    }

    @Test
    public void testForwardedWithArrowIndividualStrings() {
        String[] forwardedFields = {"f0->f1", "f1->f2"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, threeIntTupleType, fiveIntTupleType);

        assertThat(sp.getForwardingTargetFields(0, 0).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(2)).isTrue();
    }

    @Test
    public void testForwardedWithArrowOneString() {
        String[] forwardedFields = {"f0->f0;f1->f2"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, threeIntTupleType, fiveIntTupleType);

        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(2)).isTrue();

        forwardedFields[0] = "0->0;1->2";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, threeIntTupleType, fiveIntTupleType);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(2)).isTrue();
    }

    @Test
    public void testForwardedWithArrowSpaces() {
        String[] forwardedFields = {"  f0 ->  f0    ;   f1  -> f2 "};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, threeIntTupleType, fiveIntTupleType);

        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(2)).isTrue();
    }

    @Test
    public void testForwardedMixedOneString() {
        String[] forwardedFields = {"f2;f3;f0->f4;f4->f0"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, fiveIntTupleType, fiveIntTupleType);

        assertThat(sp.getForwardingTargetFields(0, 2).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 0).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 4).contains(0)).isTrue();
    }

    @Test
    public void testForwardedBasicType() {
        String[] forwardedFields = {"f1->*"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, threeIntTupleType, intType);

        assertThat(sp.getForwardingTargetFields(0, 0).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).size() == 0).isTrue();

        forwardedFields[0] = "*->f2";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, intType, threeIntTupleType);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(2)).isTrue();

        forwardedFields[0] = "*->*";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, intType, intType);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
    }

    @Test
    public void testForwardedWildCard() {
        String[] forwardedFields = {"*"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, threeIntTupleType, threeIntTupleType);

        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).size() == 0).isTrue();

        forwardedFields[0] = "*";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, deepNestedTupleType, deepNestedTupleType);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 4).contains(4)).isTrue();
    }

    @Test
    public void testForwardedNestedTuples() {
        String[] forwardedFields = {"f0->f0.f0; f1->f0.f1; f2->f0.f2"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, threeIntTupleType, nestedTupleType);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(2)).isTrue();

        forwardedFields[0] = "f0.f0->f1.f0.f2; f0.f1->f2; f2->f1.f2; f1->f0";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, nestedTupleType, deepNestedTupleType);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(6)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 4).contains(5)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(0)).isTrue();

        forwardedFields[0] = "0.0->1.0.2; 0.1->2; 2->1.2; 1->0";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, nestedTupleType, deepNestedTupleType);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(6)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 4).contains(5)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(0)).isTrue();

        forwardedFields[0] = "f1.f0.*->f0.*; f0->f2";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, deepNestedTupleType, nestedTupleType);
        assertThat(sp.getForwardingTargetFields(0, 1).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 0).contains(4)).isTrue();

        forwardedFields[0] = "1.0.*->0.*; 0->2";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, deepNestedTupleType, nestedTupleType);
        assertThat(sp.getForwardingTargetFields(0, 1).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 0).contains(4)).isTrue();

        forwardedFields[0] = "f1.f0->f0; f0->f2";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, deepNestedTupleType, nestedTupleType);
        assertThat(sp.getForwardingTargetFields(0, 1).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 0).contains(4)).isTrue();

        forwardedFields[0] = "1.0->0; 0->2";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, deepNestedTupleType, nestedTupleType);
        assertThat(sp.getForwardingTargetFields(0, 1).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 0).contains(4)).isTrue();

        forwardedFields[0] = "f1.f0.f1; f1.f1; f2";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, deepNestedTupleType, deepNestedTupleType);
        assertThat(sp.getForwardingTargetFields(0, 2).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 4).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 6).contains(6)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 0).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 5).size() == 0).isTrue();

        forwardedFields[0] = "f1.f0.*; f1.f2";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, deepNestedTupleType, deepNestedTupleType);
        assertThat(sp.getForwardingTargetFields(0, 1).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 5).contains(5)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 0).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 4).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 6).size() == 0).isTrue();
    }

    @Test
    public void testForwardedPojo() {

        String[] forwardedFields = {"int1->int2; int3->int1; string1 "};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, pojoType, pojoType);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(3)).isTrue();

        forwardedFields[0] = "f1->int1; f0->int3 ";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, threeIntTupleType, pojoType);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(0)).isTrue();

        forwardedFields[0] = "int1->f2; int2->f0; int3->f1";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, pojoType, threeIntTupleType);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(1)).isTrue();

        forwardedFields[0] = "*->pojo1.*";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, pojoType, nestedPojoType);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(4)).isTrue();

        forwardedFields[0] = "*->pojo1";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, pojoType, nestedPojoType);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(4)).isTrue();

        forwardedFields[0] = "int1; string1; int2->pojo1.int3";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, pojoType, nestedPojoType);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(5)).isTrue();

        forwardedFields[0] = "pojo1.*->f2.*; int1->f1";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, nestedPojoType, pojoInTupleType);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 4).contains(5)).isTrue();

        forwardedFields[0] = "f2.*->*";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, pojoInTupleType, pojoType);
        assertThat(sp.getForwardingTargetFields(0, 2).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 4).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 5).contains(3)).isTrue();

        forwardedFields[0] = "pojo1->f2; int1->f1";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, nestedPojoType, pojoInTupleType);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 4).contains(5)).isTrue();

        forwardedFields[0] = "f2->*";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, pojoInTupleType, pojoType);
        assertThat(sp.getForwardingTargetFields(0, 2).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 4).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 5).contains(3)).isTrue();

        forwardedFields[0] = "int2; string1";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, pojoType, pojoType);
        assertThat(sp.getForwardingTargetFields(0, 1).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 0).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).size() == 0).isTrue();

        forwardedFields[0] = "pojo1.int1; string1";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, nestedPojoType, nestedPojoType);
        assertThat(sp.getForwardingTargetFields(0, 1).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 5).contains(5)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 0).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 4).size() == 0).isTrue();

        forwardedFields[0] = "pojo1.*; int1";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, nestedPojoType, nestedPojoType);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 4).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 5).size() == 0).isTrue();

        forwardedFields[0] = "pojo1; int1";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, nestedPojoType, nestedPojoType);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 4).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 5).size() == 0).isTrue();
    }

    @Test(expected = InvalidSemanticAnnotationException.class)
    public void testInvalidPojoField() {
        String[] forwardedFields = {"invalidField"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, pojoType, threeIntTupleType);
    }

    @Test(expected = InvalidSemanticAnnotationException.class)
    public void testForwardedNoArrowOneStringInvalidDelimiter() {
        String[] forwardedFields = {"f2,f3,f0"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, fiveIntTupleType, fiveIntTupleType);
    }

    @Test(expected = InvalidSemanticAnnotationException.class)
    public void testForwardedSameTargetTwice() {
        String[] forwardedFields = {"f0->f2; f1->f2"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, fiveIntTupleType, fiveIntTupleType);
    }

    @Test(expected = InvalidSemanticAnnotationException.class)
    public void testForwardedInvalidTargetFieldType1() {
        String[] forwardedFields = {"f0->f0", "f1->f2"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, fiveIntTupleType, threeMixedTupleType);
    }

    @Test(expected = InvalidSemanticAnnotationException.class)
    public void testForwardedInvalidTargetFieldType2() {
        String[] forwardedFields = {"f2.*->*"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, pojoInTupleType, pojo2Type);
    }

    @Test(expected = InvalidSemanticAnnotationException.class)
    public void testForwardedInvalidTargetFieldType3() {
        String[] forwardedFields = {"*"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, pojoInTupleType, pojo2Type);
    }

    @Test(expected = InvalidSemanticAnnotationException.class)
    public void testForwardedInvalidTargetFieldType4() {
        String[] forwardedFields = {"int1; string1"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, pojoInTupleType, pojo2Type);
    }

    @Test(expected = InvalidSemanticAnnotationException.class)
    public void testForwardedInvalidTargetFieldType5() {
        String[] forwardedFields = {"f0.*->*"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, nestedTupleType, fiveIntTupleType);
    }

    @Test(expected = InvalidSemanticAnnotationException.class)
    public void testForwardedWildCardInvalidTypes1() {
        String[] forwardedFields = {"*"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, fiveIntTupleType, threeIntTupleType);
    }

    @Test(expected = InvalidSemanticAnnotationException.class)
    public void testForwardedWildCardInvalidTypes2() {
        String[] forwardedFields = {"*"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, threeIntTupleType, fiveIntTupleType);
    }

    @Test(expected = InvalidSemanticAnnotationException.class)
    public void testForwardedWildCardInvalidTypes3() {
        String[] forwardedFields = {"*"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, pojoType, pojo2Type);
    }

    @Test(expected = InvalidSemanticAnnotationException.class)
    public void testForwardedForwardWildCard() {
        String[] forwardedFields = {"f1->*"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, threeIntTupleType, threeIntTupleType);
    }

    @Test(expected = InvalidSemanticAnnotationException.class)
    public void testForwardedInvalidExpression() {
        String[] forwardedFields = {"f0"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, intType, threeIntTupleType);
    }

    @Test(expected = InvalidSemanticAnnotationException.class)
    public void testForwardedForwardMultiFields() {
        String[] forwardedFields = {"f1->f0,f1"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, threeIntTupleType, threeIntTupleType);
    }

    @Test(expected = InvalidSemanticAnnotationException.class)
    public void testForwardedInvalidString() {
        String[] forwardedFields = {"notValid"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, null, threeIntTupleType, threeIntTupleType);
    }

    // --------------------------------------------------------------------------------------------
    // Non-Forwarded Fields Annotation
    // --------------------------------------------------------------------------------------------

    @Test
    public void testNonForwardedIndividualStrings() {
        String[] nonForwardedFields = {"f1", "f2"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, nonForwardedFields, null, threeIntTupleType, threeIntTupleType);

        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).size() == 0).isTrue();
    }

    @Test
    public void testNonForwardedSingleString() {
        String[] nonForwardedFields = {"f1;f2"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, nonForwardedFields, null, threeIntTupleType, threeIntTupleType);

        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).size() == 0).isTrue();

        nonForwardedFields[0] = "f1;f2;";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, nonForwardedFields, null, threeIntTupleType, threeIntTupleType);

        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).size() == 0).isTrue();
    }

    @Test
    public void testNonForwardedSpaces() {
        String[] nonForwardedFields = {" f1 ;   f2"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, nonForwardedFields, null, threeIntTupleType, threeIntTupleType);

        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).size() == 0).isTrue();
    }

    @Test
    public void testNonForwardedNone() {
        String[] nonForwardedFields = {""};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, nonForwardedFields, null, threeIntTupleType, threeIntTupleType);

        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(2)).isTrue();
    }

    @Test
    public void testNonForwardedNestedTuple() {
        String[] nonForwardedFields = {"f1.f0.*; f1.f2; f0"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, nonForwardedFields, null, deepNestedTupleType, deepNestedTupleType);

        assertThat(sp.getForwardingTargetFields(0, 0).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 4).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 5).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 6).contains(6)).isTrue();

        nonForwardedFields[0] = "f1.f0; f1.f2; f0";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, nonForwardedFields, null, deepNestedTupleType, deepNestedTupleType);

        assertThat(sp.getForwardingTargetFields(0, 0).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 4).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 5).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 6).contains(6)).isTrue();

        nonForwardedFields[0] = "f2; f1.f1";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, nonForwardedFields, null, deepNestedTupleType, deepNestedTupleType);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 4).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 5).contains(5)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 6).size() == 0).isTrue();
    }

    @Test
    public void testNonForwardedPojo() {
        String[] nonForwardedFields = {"int1; string1"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, nonForwardedFields, null, pojoType, pojoType);

        assertThat(sp.getForwardingTargetFields(0, 0).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).contains(2)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).size() == 0).isTrue();
    }

    @Test
    public void testNonForwardedNestedPojo() {
        String[] nonForwardedFields = {"int1; pojo1.*"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, nonForwardedFields, null, nestedPojoType, nestedPojoType);

        assertThat(sp.getForwardingTargetFields(0, 0).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 4).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 5).contains(5)).isTrue();

        nonForwardedFields[0] = "pojo1.int2; string1";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, nonForwardedFields, null, nestedPojoType, nestedPojoType);
        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(1)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 3).contains(3)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 4).contains(4)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 5).size() == 0).isTrue();
    }

    @Test(expected = InvalidSemanticAnnotationException.class)
    public void testNonForwardedInvalidTypes1() {
        String[] nonForwardedFields = {"f1; f2"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, nonForwardedFields, null, threeIntTupleType, nestedPojoType);
    }

    @Test(expected = InvalidSemanticAnnotationException.class)
    public void testNonForwardedInvalidTypes2() {
        String[] nonForwardedFields = {"f1; f2"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, nonForwardedFields, null, nestedPojoType, threeIntTupleType);
    }

    @Test(expected = InvalidSemanticAnnotationException.class)
    public void testNonForwardedInvalidTypes3() {
        String[] nonForwardedFields = {"f1; f2"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, nonForwardedFields, null, threeIntTupleType, fiveIntTupleType);
    }

    @Test(expected = InvalidSemanticAnnotationException.class)
    public void testNonForwardedInvalidTypes4() {
        String[] nonForwardedFields = {"f1; f2"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, nonForwardedFields, null, fiveIntTupleType, threeIntTupleType);
    }

    @Test(expected = InvalidSemanticAnnotationException.class)
    public void testNonForwardedInvalidTypes5() {
        String[] nonForwardedFields = {"int1"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, nonForwardedFields, null, pojoType, pojo2Type);
    }

    @Test(expected = InvalidSemanticAnnotationException.class)
    public void testNonForwardedInvalidNesting() {
        String[] nonForwardedFields = {"f0.f4"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, nonForwardedFields, null, nestedTupleType, nestedTupleType);
    }

    @Test(expected = InvalidSemanticAnnotationException.class)
    public void testNonForwardedInvalidString() {
        String[] nonForwardedFields = {"notValid"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, nonForwardedFields, null, threeIntTupleType, threeIntTupleType);
    }

    // --------------------------------------------------------------------------------------------
    // Read Fields Annotation
    // --------------------------------------------------------------------------------------------

    @Test
    public void testReadFieldsIndividualStrings() {
        String[] readFields = {"f1", "f2"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, null, readFields, threeIntTupleType, threeIntTupleType);

        FieldSet fs = sp.getReadFields(0);
        assertThat(fs.size() == 2).isTrue();
        assertThat(fs.contains(2)).isTrue();
        assertThat(fs.contains(1)).isTrue();
    }

    @Test
    public void testReadFieldsOneString() {
        String[] readFields = {"f1;f2"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, null, readFields, threeIntTupleType, threeIntTupleType);

        FieldSet fs = sp.getReadFields(0);
        assertThat(fs.size() == 2).isTrue();
        assertThat(fs.contains(2)).isTrue();
        assertThat(fs.contains(1)).isTrue();

        readFields[0] = "f1;f2;";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, null, readFields, threeIntTupleType, threeIntTupleType);

        fs = sp.getReadFields(0);
        assertThat(fs.size() == 2).isTrue();
        assertThat(fs.contains(2)).isTrue();
        assertThat(fs.contains(1)).isTrue();
    }

    @Test
    public void testReadFieldsSpaces() {
        String[] readFields = {"  f1  ; f2   "};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, null, readFields, threeIntTupleType, threeIntTupleType);

        FieldSet fs = sp.getReadFields(0);
        assertThat(fs.size() == 2).isTrue();
        assertThat(fs.contains(2)).isTrue();
        assertThat(fs.contains(1)).isTrue();
    }

    @Test
    public void testReadFieldsBasic() {
        String[] readFields = {"*"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, null, readFields, intType, intType);

        FieldSet fs = sp.getReadFields(0);
        assertThat(fs.size() == 1).isTrue();
        assertThat(fs.contains(0)).isTrue();

        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, null, readFields, intType, fiveIntTupleType);

        fs = sp.getReadFields(0);
        assertThat(fs.size() == 1).isTrue();
        assertThat(fs.contains(0)).isTrue();
    }

    @Test
    public void testReadFieldsNestedTuples() {
        String[] readFields = {"f0.f1; f0.f2; f2"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, null, readFields, nestedTupleType, intType);

        FieldSet fs = sp.getReadFields(0);
        assertThat(fs.size() == 3).isTrue();
        assertThat(fs.contains(1)).isTrue();
        assertThat(fs.contains(2)).isTrue();
        assertThat(fs.contains(4)).isTrue();

        readFields[0] = "f0;f1";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, null, readFields, nestedTupleType, intType);

        fs = sp.getReadFields(0);
        assertThat(fs.size() == 4).isTrue();
        assertThat(fs.contains(0)).isTrue();
        assertThat(fs.contains(1)).isTrue();
        assertThat(fs.contains(2)).isTrue();
        assertThat(fs.contains(3)).isTrue();
    }

    @Test
    public void testReadFieldsNestedTupleWildCard() {
        String[] readFields = {"*"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, null, readFields, nestedTupleType, intType);

        FieldSet fs = sp.getReadFields(0);
        assertThat(fs.size() == 5).isTrue();
        assertThat(fs.contains(0)).isTrue();
        assertThat(fs.contains(1)).isTrue();
        assertThat(fs.contains(2)).isTrue();
        assertThat(fs.contains(3)).isTrue();
        assertThat(fs.contains(4)).isTrue();

        readFields[0] = "f0.*;f1";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, null, readFields, nestedTupleType, intType);

        fs = sp.getReadFields(0);
        assertThat(fs.size() == 4).isTrue();
        assertThat(fs.contains(0)).isTrue();
        assertThat(fs.contains(1)).isTrue();
        assertThat(fs.contains(2)).isTrue();
        assertThat(fs.contains(3)).isTrue();
    }

    @Test
    public void testReadFieldsPojo() {
        String[] readFields = {"int2; string1"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, null, readFields, pojoType, threeIntTupleType);

        FieldSet fs = sp.getReadFields(0);
        assertThat(fs.size() == 2).isTrue();
        assertThat(fs.contains(1)).isTrue();
        assertThat(fs.contains(3)).isTrue();

        readFields[0] = "*";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, null, readFields, pojoType, intType);

        fs = sp.getReadFields(0);
        assertThat(fs.size() == 4).isTrue();
        assertThat(fs.contains(0)).isTrue();
        assertThat(fs.contains(1)).isTrue();
        assertThat(fs.contains(2)).isTrue();
        assertThat(fs.contains(3)).isTrue();
    }

    @Test
    public void testReadFieldsNestedPojo() {
        String[] readFields = {"pojo1.int2; string1; pojo1.string1"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, null, readFields, nestedPojoType, intType);

        FieldSet fs = sp.getReadFields(0);
        assertThat(fs.size() == 3).isTrue();
        assertThat(fs.contains(2)).isTrue();
        assertThat(fs.contains(4)).isTrue();
        assertThat(fs.contains(5)).isTrue();

        readFields[0] = "pojo1.*";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, null, readFields, nestedPojoType, intType);

        fs = sp.getReadFields(0);
        assertThat(fs.size() == 4).isTrue();
        assertThat(fs.contains(1)).isTrue();
        assertThat(fs.contains(2)).isTrue();
        assertThat(fs.contains(3)).isTrue();
        assertThat(fs.contains(4)).isTrue();

        readFields[0] = "pojo1";
        sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, null, readFields, nestedPojoType, intType);

        fs = sp.getReadFields(0);
        assertThat(fs.size() == 4).isTrue();
        assertThat(fs.contains(1)).isTrue();
        assertThat(fs.contains(2)).isTrue();
        assertThat(fs.contains(3)).isTrue();
        assertThat(fs.contains(4)).isTrue();
    }

    @Test
    public void testReadFieldsPojoInTuple() {
        String[] readFields = {"f0; f2.int1; f2.string1"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, null, readFields, pojoInTupleType, pojo2Type);

        FieldSet fs = sp.getReadFields(0);
        assertThat(fs.size() == 3).isTrue();
        assertThat(fs.contains(0)).isTrue();
        assertThat(fs.contains(2)).isTrue();
        assertThat(fs.contains(5)).isTrue();
    }

    @Test(expected = InvalidSemanticAnnotationException.class)
    public void testReadFieldsInvalidString() {
        String[] readFields = {"notValid"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, null, readFields, threeIntTupleType, threeIntTupleType);
    }

    // --------------------------------------------------------------------------------------------
    // Two Inputs
    // --------------------------------------------------------------------------------------------

    @Test
    public void testForwardedDual() {
        String[] forwardedFieldsFirst = {"f1->f2; f2->f3"};
        String[] forwardedFieldsSecond = {"f1->f1; f2->f0"};
        DualInputSemanticProperties dsp = new DualInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsDualFromString(
                dsp,
                forwardedFieldsFirst,
                forwardedFieldsSecond,
                null,
                null,
                null,
                null,
                fourIntTupleType,
                fourIntTupleType,
                fourIntTupleType);

        assertThat(dsp.getForwardingTargetFields(0, 1).contains(2)).isTrue();
        assertThat(dsp.getForwardingTargetFields(0, 2).contains(3)).isTrue();
        assertThat(dsp.getForwardingTargetFields(1, 1).contains(1)).isTrue();
        assertThat(dsp.getForwardingTargetFields(1, 2).contains(0)).isTrue();
        assertThat(dsp.getForwardingTargetFields(0, 0).size() == 0).isTrue();
        assertThat(dsp.getForwardingTargetFields(1, 3).size() == 0).isTrue();

        forwardedFieldsFirst[0] = "f1->f0;f3->f1";
        forwardedFieldsSecond[0] = "*->f2.*";
        dsp = new DualInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsDualFromString(
                dsp,
                forwardedFieldsFirst,
                forwardedFieldsSecond,
                null,
                null,
                null,
                null,
                fourIntTupleType,
                pojoType,
                pojoInTupleType);

        assertThat(dsp.getForwardingTargetFields(0, 0).size() == 0).isTrue();
        assertThat(dsp.getForwardingTargetFields(0, 1).contains(0)).isTrue();
        assertThat(dsp.getForwardingTargetFields(0, 2).size() == 0).isTrue();
        assertThat(dsp.getForwardingTargetFields(0, 3).contains(1)).isTrue();
        assertThat(dsp.getForwardingTargetFields(1, 0).contains(2)).isTrue();
        assertThat(dsp.getForwardingTargetFields(1, 1).contains(3)).isTrue();
        assertThat(dsp.getForwardingTargetFields(1, 2).contains(4)).isTrue();
        assertThat(dsp.getForwardingTargetFields(1, 3).contains(5)).isTrue();

        forwardedFieldsFirst[0] = "f1.f0.f2->int1; f2->pojo1.int3";
        forwardedFieldsSecond[0] = "string1; int2->pojo1.int1; int1->pojo1.int2";
        dsp = new DualInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsDualFromString(
                dsp,
                forwardedFieldsFirst,
                forwardedFieldsSecond,
                null,
                null,
                null,
                null,
                deepNestedTupleType,
                pojoType,
                nestedPojoType);

        assertThat(dsp.getForwardingTargetFields(0, 0).size() == 0).isTrue();
        assertThat(dsp.getForwardingTargetFields(0, 1).size() == 0).isTrue();
        assertThat(dsp.getForwardingTargetFields(0, 2).size() == 0).isTrue();
        assertThat(dsp.getForwardingTargetFields(0, 3).contains(0)).isTrue();
        assertThat(dsp.getForwardingTargetFields(0, 4).size() == 0).isTrue();
        assertThat(dsp.getForwardingTargetFields(0, 5).size() == 0).isTrue();
        assertThat(dsp.getForwardingTargetFields(0, 6).contains(3)).isTrue();
        assertThat(dsp.getForwardingTargetFields(1, 0).contains(2)).isTrue();
        assertThat(dsp.getForwardingTargetFields(1, 1).contains(1)).isTrue();
        assertThat(dsp.getForwardingTargetFields(1, 2).size() == 0).isTrue();
        assertThat(dsp.getForwardingTargetFields(1, 3).contains(5)).isTrue();

        String[] forwardedFieldsFirst2 = {"f1.f0.f2->int1", "f2->pojo1.int3"};
        String[] forwardedFieldsSecond2 = {"string1", "int2->pojo1.int1", "int1->pojo1.int2"};
        dsp = new DualInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsDualFromString(
                dsp,
                forwardedFieldsFirst2,
                forwardedFieldsSecond2,
                null,
                null,
                null,
                null,
                deepNestedTupleType,
                pojoType,
                nestedPojoType);

        assertThat(dsp.getForwardingTargetFields(0, 0).size() == 0).isTrue();
        assertThat(dsp.getForwardingTargetFields(0, 1).size() == 0).isTrue();
        assertThat(dsp.getForwardingTargetFields(0, 2).size() == 0).isTrue();
        assertThat(dsp.getForwardingTargetFields(0, 3).contains(0)).isTrue();
        assertThat(dsp.getForwardingTargetFields(0, 4).size() == 0).isTrue();
        assertThat(dsp.getForwardingTargetFields(0, 5).size() == 0).isTrue();
        assertThat(dsp.getForwardingTargetFields(0, 6).contains(3)).isTrue();
        assertThat(dsp.getForwardingTargetFields(1, 0).contains(2)).isTrue();
        assertThat(dsp.getForwardingTargetFields(1, 1).contains(1)).isTrue();
        assertThat(dsp.getForwardingTargetFields(1, 2).size() == 0).isTrue();
        assertThat(dsp.getForwardingTargetFields(1, 3).contains(5)).isTrue();
    }

    @Test
    public void testNonForwardedDual() {
        String[] nonForwardedFieldsFirst = {"f1;f2"};
        String[] nonForwardedFieldsSecond = {"f0"};
        DualInputSemanticProperties dsp = new DualInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsDualFromString(
                dsp,
                null,
                null,
                nonForwardedFieldsFirst,
                nonForwardedFieldsSecond,
                null,
                null,
                threeIntTupleType,
                threeIntTupleType,
                threeIntTupleType);

        assertThat(dsp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(dsp.getForwardingTargetFields(0, 1).size() == 0).isTrue();
        assertThat(dsp.getForwardingTargetFields(0, 2).size() == 0).isTrue();
        assertThat(dsp.getForwardingTargetFields(1, 0).size() == 0).isTrue();
        assertThat(dsp.getForwardingTargetFields(1, 1).contains(1)).isTrue();
        assertThat(dsp.getForwardingTargetFields(1, 2).contains(2)).isTrue();

        nonForwardedFieldsFirst[0] = "f1";
        nonForwardedFieldsSecond[0] = "";
        dsp = new DualInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsDualFromString(
                dsp,
                null,
                null,
                nonForwardedFieldsFirst,
                null,
                null,
                null,
                threeIntTupleType,
                fiveIntTupleType,
                threeIntTupleType);

        assertThat(dsp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(dsp.getForwardingTargetFields(0, 1).size() == 0).isTrue();
        assertThat(dsp.getForwardingTargetFields(0, 2).contains(2)).isTrue();
        assertThat(dsp.getForwardingTargetFields(1, 0).size() == 0).isTrue();
        assertThat(dsp.getForwardingTargetFields(1, 1).size() == 0).isTrue();
        assertThat(dsp.getForwardingTargetFields(1, 2).size() == 0).isTrue();

        nonForwardedFieldsFirst[0] = "";
        nonForwardedFieldsSecond[0] = "f2;f0";
        dsp = new DualInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsDualFromString(
                dsp,
                null,
                null,
                null,
                nonForwardedFieldsSecond,
                null,
                null,
                fiveIntTupleType,
                threeIntTupleType,
                threeIntTupleType);

        assertThat(dsp.getForwardingTargetFields(0, 0).size() == 0).isTrue();
        assertThat(dsp.getForwardingTargetFields(0, 1).size() == 0).isTrue();
        assertThat(dsp.getForwardingTargetFields(0, 2).size() == 0).isTrue();
        assertThat(dsp.getForwardingTargetFields(1, 0).size() == 0).isTrue();
        assertThat(dsp.getForwardingTargetFields(1, 1).contains(1)).isTrue();
        assertThat(dsp.getForwardingTargetFields(1, 2).size() == 0).isTrue();

        String[] nonForwardedFields = {"f1", "f3"};
        dsp = new DualInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsDualFromString(
                dsp,
                null,
                null,
                nonForwardedFields,
                null,
                null,
                null,
                fiveIntTupleType,
                threeIntTupleType,
                fiveIntTupleType);

        assertThat(dsp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(dsp.getForwardingTargetFields(0, 1).size() == 0).isTrue();
        assertThat(dsp.getForwardingTargetFields(0, 2).contains(2)).isTrue();
        assertThat(dsp.getForwardingTargetFields(0, 3).size() == 0).isTrue();
        assertThat(dsp.getForwardingTargetFields(0, 4).contains(4)).isTrue();

        dsp = new DualInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsDualFromString(
                dsp,
                null,
                null,
                null,
                nonForwardedFields,
                null,
                null,
                threeIntTupleType,
                fiveIntTupleType,
                fiveIntTupleType);

        assertThat(dsp.getForwardingTargetFields(1, 0).contains(0)).isTrue();
        assertThat(dsp.getForwardingTargetFields(1, 1).size() == 0).isTrue();
        assertThat(dsp.getForwardingTargetFields(1, 2).contains(2)).isTrue();
        assertThat(dsp.getForwardingTargetFields(1, 3).size() == 0).isTrue();
        assertThat(dsp.getForwardingTargetFields(1, 4).contains(4)).isTrue();
    }

    @Test(expected = InvalidSemanticAnnotationException.class)
    public void testNonForwardedDualInvalidTypes1() {

        String[] nonForwardedFieldsFirst = {"f1"};
        DualInputSemanticProperties dsp = new DualInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsDualFromString(
                dsp,
                null,
                null,
                nonForwardedFieldsFirst,
                null,
                null,
                null,
                fiveIntTupleType,
                threeIntTupleType,
                threeIntTupleType);
    }

    @Test(expected = InvalidSemanticAnnotationException.class)
    public void testNonForwardedDualInvalidTypes2() {

        String[] nonForwardedFieldsSecond = {"f1"};
        DualInputSemanticProperties dsp = new DualInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsDualFromString(
                dsp,
                null,
                null,
                null,
                nonForwardedFieldsSecond,
                null,
                null,
                threeIntTupleType,
                pojoInTupleType,
                threeIntTupleType);
    }

    @Test
    public void testReadFieldsDual() {
        String[] readFieldsFirst = {"f1;f2"};
        String[] readFieldsSecond = {"f0"};
        DualInputSemanticProperties dsp = new DualInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsDualFromString(
                dsp,
                null,
                null,
                null,
                null,
                readFieldsFirst,
                readFieldsSecond,
                threeIntTupleType,
                threeIntTupleType,
                threeIntTupleType);

        assertThat(dsp.getReadFields(0).size() == 2).isTrue();
        assertThat(dsp.getReadFields(0).contains(1)).isTrue();
        assertThat(dsp.getReadFields(0).contains(2)).isTrue();
        assertThat(dsp.getReadFields(1).size() == 1).isTrue();
        assertThat(dsp.getReadFields(1).contains(0)).isTrue();

        readFieldsFirst[0] = "f0.*; f2";
        readFieldsSecond[0] = "int1; string1";
        dsp = new DualInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsDualFromString(
                dsp,
                null,
                null,
                null,
                null,
                readFieldsFirst,
                readFieldsSecond,
                nestedTupleType,
                pojoType,
                threeIntTupleType);

        assertThat(dsp.getReadFields(0).size() == 4).isTrue();
        assertThat(dsp.getReadFields(0).contains(0)).isTrue();
        assertThat(dsp.getReadFields(0).contains(1)).isTrue();
        assertThat(dsp.getReadFields(0).contains(2)).isTrue();
        assertThat(dsp.getReadFields(0).contains(4)).isTrue();
        assertThat(dsp.getReadFields(1).size() == 2).isTrue();
        assertThat(dsp.getReadFields(1).contains(0)).isTrue();
        assertThat(dsp.getReadFields(1).contains(3)).isTrue();

        readFieldsFirst[0] = "pojo1.int2; string1";
        readFieldsSecond[0] = "f2.int2";
        dsp = new DualInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsDualFromString(
                dsp,
                null,
                null,
                null,
                null,
                readFieldsFirst,
                readFieldsSecond,
                nestedPojoType,
                pojoInTupleType,
                threeIntTupleType);

        assertThat(dsp.getReadFields(0).size() == 2).isTrue();
        assertThat(dsp.getReadFields(0).contains(2)).isTrue();
        assertThat(dsp.getReadFields(0).contains(5)).isTrue();
        assertThat(dsp.getReadFields(1).size() == 1).isTrue();
        assertThat(dsp.getReadFields(1).contains(3)).isTrue();

        String[] readFields = {"f0", "f2", "f4"};
        dsp = new DualInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsDualFromString(
                dsp,
                null,
                null,
                null,
                null,
                readFields,
                readFields,
                fiveIntTupleType,
                fiveIntTupleType,
                threeIntTupleType);

        assertThat(dsp.getReadFields(0).size() == 3).isTrue();
        assertThat(dsp.getReadFields(0).contains(0)).isTrue();
        assertThat(dsp.getReadFields(0).contains(2)).isTrue();
        assertThat(dsp.getReadFields(0).contains(4)).isTrue();
        assertThat(dsp.getReadFields(1).size() == 3).isTrue();
        assertThat(dsp.getReadFields(1).contains(0)).isTrue();
        assertThat(dsp.getReadFields(1).contains(2)).isTrue();
        assertThat(dsp.getReadFields(1).contains(4)).isTrue();
    }

    // --------------------------------------------------------------------------------------------
    // Mixed Annotations
    // --------------------------------------------------------------------------------------------

    @Test
    public void testForwardedRead() {
        String[] forwardedFields = {"f0->f0;f1->f2"};
        String[] readFields = {"f0; f2"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, forwardedFields, null, readFields, threeIntTupleType, fiveIntTupleType);

        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).contains(2)).isTrue();
        assertThat(sp.getReadFields(0).size() == 2).isTrue();
        assertThat(sp.getReadFields(0).contains(0)).isTrue();
        assertThat(sp.getReadFields(0).contains(2)).isTrue();
    }

    @Test
    public void testNonForwardedRead() {
        String[] nonForwardedFields = {"f1;f2"};
        String[] readFields = {"f0; f2"};
        SingleInputSemanticProperties sp = new SingleInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsSingleFromString(
                sp, null, nonForwardedFields, readFields, threeIntTupleType, threeIntTupleType);

        assertThat(sp.getForwardingTargetFields(0, 0).contains(0)).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 1).size() == 0).isTrue();
        assertThat(sp.getForwardingTargetFields(0, 2).size() == 0).isTrue();
        assertThat(sp.getReadFields(0).size() == 2).isTrue();
        assertThat(sp.getReadFields(0).contains(0)).isTrue();
        assertThat(sp.getReadFields(0).contains(2)).isTrue();
    }

    @Test
    public void testForwardedReadDual() {
        String[] forwardedFieldsFirst = {"f1->f2; f2->f3"};
        String[] forwardedFieldsSecond = {"f1->f1; f2->f0"};
        String[] readFieldsFirst = {"0;2"};
        String[] readFieldsSecond = {"1"};
        DualInputSemanticProperties dsp = new DualInputSemanticProperties();
        SemanticPropUtil.getSemanticPropsDualFromString(
                dsp,
                forwardedFieldsFirst,
                forwardedFieldsSecond,
                null,
                null,
                readFieldsFirst,
                readFieldsSecond,
                fourIntTupleType,
                fourIntTupleType,
                fourIntTupleType);

        assertThat(dsp.getForwardingTargetFields(0, 1).contains(2)).isTrue();
        assertThat(dsp.getForwardingTargetFields(0, 2).contains(3)).isTrue();
        assertThat(dsp.getForwardingTargetFields(1, 1).contains(1)).isTrue();
        assertThat(dsp.getForwardingTargetFields(1, 2).contains(0)).isTrue();
        assertThat(dsp.getForwardingTargetFields(0, 0).size() == 0).isTrue();
        assertThat(dsp.getForwardingTargetFields(1, 3).size() == 0).isTrue();
        assertThat(dsp.getReadFields(0).size() == 2).isTrue();
        assertThat(dsp.getReadFields(0).contains(0)).isTrue();
        assertThat(dsp.getReadFields(0).contains(2)).isTrue();
        assertThat(dsp.getReadFields(1).size() == 1).isTrue();
        assertThat(dsp.getReadFields(1).contains(1)).isTrue();
    }

    @Test(expected = InvalidSemanticAnnotationException.class)
    public void testForwardedNonForwardedCheck() {
        String[] forwarded = {"1"};
        String[] nonForwarded = {"1"};
        SemanticPropUtil.getSemanticPropsSingleFromString(
                new SingleInputSemanticProperties(),
                forwarded,
                nonForwarded,
                null,
                threeIntTupleType,
                threeIntTupleType);
    }

    @Test(expected = InvalidSemanticAnnotationException.class)
    public void testForwardedNonForwardedFirstCheck() {
        String[] forwarded = {"1"};
        String[] nonForwarded = {"1"};
        SemanticPropUtil.getSemanticPropsDualFromString(
                new DualInputSemanticProperties(),
                forwarded,
                null,
                nonForwarded,
                null,
                null,
                null,
                threeIntTupleType,
                threeIntTupleType,
                threeIntTupleType);
    }

    @Test(expected = InvalidSemanticAnnotationException.class)
    public void testForwardedNonForwardedSecondCheck() {
        String[] forwarded = {"1"};
        String[] nonForwarded = {"1"};
        SemanticPropUtil.getSemanticPropsDualFromString(
                new DualInputSemanticProperties(),
                null,
                forwarded,
                null,
                nonForwarded,
                null,
                null,
                threeIntTupleType,
                threeIntTupleType,
                threeIntTupleType);
    }

    // --------------------------------------------------------------------------------------------
    // Pojo Type Classes
    // --------------------------------------------------------------------------------------------

    /** Sample test pojo. */
    public static class TestPojo {

        public int int1;
        public int int2;
        public int int3;
        public String string1;
    }

    /** Sample test pojo. */
    public static class TestPojo2 {

        public int myInt1;
        public int myInt2;
        public int myInt3;
        public String myString1;
    }

    /** Sample test pojo with nested type. */
    public static class NestedTestPojo {

        public int int1;
        public TestPojo pojo1;
        public String string1;
    }
}
