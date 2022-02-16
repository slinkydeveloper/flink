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

package org.apache.flink.table.planner.codegen.calls

import org.apache.flink.table.planner.codegen.CodeGenUtils.{className, primitiveDefaultValue, primitiveTypeTermForType}
import org.apache.flink.table.planner.codegen.calls.ScalarOperatorGens.toCodegenCastContext
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.planner.functions.casting.{CastCodeBlock, CastRuleProvider, CodeGeneratorCastRule, ExpressionCodeGeneratorCastRule}
import org.apache.flink.table.types.logical.LogicalType

/**
  * Generates IF function call.
  */
class IfCallGen() extends CallGenerator {

  override def generate(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      returnType: LogicalType)
    : GeneratedExpression = {

    // Inferred return type is ARG1. Must be the same as ARG2.
    // This is a temporary solution which introduce type cast in codegen.
    // Not elegant, but can allow IF function to handle different numeric type arguments.
    val castedResultTerm1 = normalizeArgument(ctx, operands(1), returnType)
    val castedResultTerm2 = normalizeArgument(ctx, operands(2), returnType)
    if (castedResultTerm1 == null || castedResultTerm2 == null) {
      throw new Exception(String.format("Unsupported operand types: IF(boolean, %s, %s)",
        operands(1).resultType, operands(2).resultType))
    }

    val resultTypeTerm = primitiveTypeTermForType(returnType)
    val resultDefault = primitiveDefaultValue(returnType)
    val Seq(resultTerm, nullTerm) = ctx.addReusableLocalVariables(
      (resultTypeTerm, "result"),
      ("boolean", "isNull"))

    val resultCode =
      s"""
         |// --- Start code generated by ${className[IfCallGen]}
         |${castedResultTerm1.getCode}
         |${castedResultTerm2.getCode}
         |${operands.head.code}
         |$resultTerm = $resultDefault;
         |if (${operands.head.resultTerm}) {
         |  ${operands(1).code}
         |  if (!${operands(1).nullTerm}) {
         |    $resultTerm = ${castedResultTerm1.getReturnTerm};
         |  }
         |  $nullTerm = ${operands(1).nullTerm};
         |} else {
         |  ${operands(2).code}
         |  if (!${operands(2).nullTerm}) {
         |    $resultTerm = ${castedResultTerm2.getReturnTerm};
         |  }
         |  $nullTerm = ${operands(2).nullTerm};
         |}
         |// --- End code generated by ${className[IfCallGen]}
       """.stripMargin

    GeneratedExpression(resultTerm, nullTerm, resultCode, returnType)
  }

  /**
   * This function will return the argument term casted if an expression casting can be performed,
   * or null if no casting can be performed
   */
  private def normalizeArgument(
      ctx: CodeGeneratorContext,
      expr: GeneratedExpression,
      targetType: LogicalType): CastCodeBlock = {

      val rule = CastRuleProvider.resolveRule(expr.resultType, targetType)
      rule match {
        case codeGeneratorCastRule: ExpressionCodeGeneratorCastRule[_, _] =>
          CastCodeBlock.withoutCode(codeGeneratorCastRule.generateExpression(
            toCodegenCastContext(ctx),
            expr.resultTerm,
            expr.resultType,
            targetType
          ), expr.nullTerm)
        case codeGeneratorCastRule: CodeGeneratorCastRule[_, _] =>
          codeGeneratorCastRule.generateCodeBlock(
            toCodegenCastContext(ctx),
            expr.resultTerm,
            expr.nullTerm,
            expr.resultType,
            targetType
          )
        case _ => null
      }
  }
}
