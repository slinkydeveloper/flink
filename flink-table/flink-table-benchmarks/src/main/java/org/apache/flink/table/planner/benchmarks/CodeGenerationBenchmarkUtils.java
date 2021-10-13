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

package org.apache.flink.table.planner.benchmarks;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.planner.calcite.FlinkRexBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.codegen.CalcCodeGenerator;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexNode;

import java.util.Collections;
import java.util.Optional;

/** Utils to execute code generation in benchmarks. */
class CodeGenerationBenchmarkUtils {

    public static FlatMapFunction<RowData, RowData> generateCastFunction(
            LogicalType inputType, LogicalType targetType) {
        final RelDataTypeSystem typeSystem = new FlinkTypeSystem();
        final FlinkTypeFactory typeFactory = new FlinkTypeFactory(typeSystem);
        final FlinkRexBuilder rexBuilder = new FlinkRexBuilder(typeFactory);

        final RexNode castExpression =
                rexBuilder.makeCall(
                        typeFactory.createFieldTypeFromLogicalType(targetType),
                        FlinkSqlOperatorTable.CAST,
                        Collections.singletonList(
                                rexBuilder.makeFieldAccess(
                                        rexBuilder.makeInputRef(
                                                typeFactory.buildRelNodeRowType(
                                                        RowType.of(inputType)),
                                                0),
                                        0)));

        GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedFunction =
                CalcCodeGenerator.generateFunction(
                        RowType.of(inputType),
                        "MyBenchmarkFn",
                        RowType.of(targetType),
                        BinaryRowData.class,
                        JavaScalaConversionUtil.toScala(Collections.singletonList(castExpression)),
                        JavaScalaConversionUtil.toScala(Optional.empty()),
                        new TableConfig());

        System.out.println("Compiling function with code:\n" + generatedFunction.getCode());

        generatedFunction.compile(UpcastingWithPrimitivesBenchmark.class.getClassLoader());
        return generatedFunction.newInstance(
                UpcastingWithPrimitivesBenchmark.class.getClassLoader());
    }
}
