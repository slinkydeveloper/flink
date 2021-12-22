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

package org.apache.flink.table.planner.functions.casting;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.data.utils.CastExecutor;
import org.apache.flink.table.planner.codegen.CodeGenUtils;
import org.apache.flink.table.runtime.generated.CompileUtils;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.planner.codegen.CodeGenUtils.className;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.cast;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.constructorCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.strLiteral;

/**
 * Base class for {@link CastRule} supporting code generation. This base class implements {@link
 * #create(CastRule.Context, LogicalType, LogicalType)} compiling the generated code block into a
 * {@link CastExecutor} implementation.
 *
 * <p>It is suggested to implement {@link CodeGeneratorCastRule} starting from {@link
 * AbstractNullAwareCodeGeneratorCastRule}, which provides nullability checks, or from {@link
 * AbstractExpressionCodeGeneratorCastRule} to generate simple expression casts.
 */
abstract class AbstractCodeGeneratorCastRule<IN, OUT> extends AbstractCastRule<IN, OUT>
        implements CodeGeneratorCastRule<IN, OUT> {

    protected AbstractCodeGeneratorCastRule(CastRulePredicate predicate) {
        super(predicate);
    }

    @SuppressWarnings("unchecked")
    @Override
    public CastExecutor<IN, OUT> create(
            CastRule.Context castRuleContext,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        final String inputTerm = "_myInput";
        final String inputIsNullTerm = "_myInputIsNull";
        final String castExecutorClassName = CodeGenUtils.newName("GeneratedCastExecutor");
        final String inputTypeTerm = CodeGenUtils.boxedTypeTermForType(inputLogicalType);

        final CastExecutorCodeGeneratorContext ctx =
                new CastExecutorCodeGeneratorContext(castRuleContext);
        final CastCodeBlock codeBlock =
                generateCodeBlock(
                        ctx, inputTerm, inputIsNullTerm, inputLogicalType, targetLogicalType);

        // Class fields can contain type serializers
        final String classFieldDecls = String.join("\n", ctx.getClassFields());

        final Stream<String> constructorArgsDeclaration =
                Stream.concat(
                        Stream.of("ClassLoader classLoader"),
                        ctx.constructorArguments.stream()
                                .map(e -> className(e.getValue().getClass()) + " " + e.getKey()));
        final String constructorSignature =
                "public "
                        + castExecutorClassName
                        + "("
                        + constructorArgsDeclaration.collect(Collectors.joining(", "))
                        + ")";
        final CastRuleUtils.CodeWriter constructorBodyWriter = new CastRuleUtils.CodeWriter();
        // Assign constructor arguments
        ctx.constructorArguments.forEach(
                e -> constructorBodyWriter.assignStmt("this." + e.getKey(), e.getKey()));
        // Invoke open for DataStructureConverter
        ctx.dataStructureConverters.values().stream()
                .map(Map.Entry::getKey)
                .forEach(
                        converterTerm ->
                                constructorBodyWriter.stmt(
                                        methodCall(converterTerm, "open", "classLoader")));

        // Because janino doesn't support generics, we need to manually cast the input variable of
        // the cast method
        final String functionSignature =
                "@Override public Object cast(Object _myInputObj) throws "
                        + className(TableException.class);

        // Write the function body
        final CastRuleUtils.CodeWriter bodyWriter = new CastRuleUtils.CodeWriter();
        bodyWriter.declStmt(inputTypeTerm, inputTerm, cast(inputTypeTerm, "_myInputObj"));
        bodyWriter.declStmt("boolean", inputIsNullTerm, "_myInputObj == null");
        ctx.variableDeclarationStatements.forEach(decl -> bodyWriter.appendBlock(decl + "\n"));

        if (this.canFail()) {
            bodyWriter.tryCatchStmt(
                    tryWriter ->
                            tryWriter.append(codeBlock).stmt("return " + codeBlock.getReturnTerm()),
                    (exceptionTerm, catchWriter) ->
                            catchWriter.throwStmt(
                                    constructorCall(
                                            TableException.class,
                                            strLiteral(
                                                    "Error when casting "
                                                            + inputLogicalType
                                                            + " to "
                                                            + targetLogicalType
                                                            + "."),
                                            exceptionTerm)));
        } else {
            bodyWriter.append(codeBlock).stmt("return " + codeBlock.getReturnTerm());
        }

        final String classCode =
                "public final class "
                        + castExecutorClassName
                        + " implements "
                        + className(CastExecutor.class)
                        + " {\n"
                        + classFieldDecls
                        + "\n"
                        + constructorSignature
                        + " {\n"
                        + constructorBodyWriter
                        + "}\n"
                        + functionSignature
                        + " {\n"
                        + bodyWriter
                        + "}\n}";

        try {
            Object[] constructorArgs =
                    Stream.concat(
                                    Stream.of(castRuleContext.getClassLoader()),
                                    ctx.constructorArguments.stream().map(Map.Entry::getValue))
                            .toArray(Object[]::new);
            return (CastExecutor<IN, OUT>)
                    CompileUtils.compile(
                                    castRuleContext.getClassLoader(),
                                    castExecutorClassName,
                                    classCode)
                            .getConstructors()[0]
                            .newInstance(constructorArgs);
        } catch (Throwable e) {
            throw new FlinkRuntimeException(
                    "Cast executor cannot be instantiated. This is a bug. Please file an issue. Code:\n"
                            + classCode,
                    e);
        }
    }

    private static final class CastExecutorCodeGeneratorContext
            implements CodeGeneratorCastRule.Context {

        private final CastRule.Context castRuleCtx;

        private final Map<LogicalType, Map.Entry<String, TypeSerializer<?>>> typeSerializers =
                new LinkedHashMap<>();
        private final Map<LogicalType, Map.Entry<String, DataStructureConverter<Object, Object>>>
                dataStructureConverters = new LinkedHashMap<>();

        private final List<Map.Entry<String, Object>> constructorArguments = new ArrayList<>();
        private final List<String> variableDeclarationStatements = new ArrayList<>();
        private final List<String> classFields = new ArrayList<>();
        private int variableIndex = 0;

        private CastExecutorCodeGeneratorContext(CastRule.Context castRuleCtx) {
            this.castRuleCtx = castRuleCtx;
        }

        @Override
        public boolean legacyBehaviour() {
            return castRuleCtx.legacyBehaviour();
        }

        @Override
        public String getSessionTimeZoneTerm() {
            return "java.util.TimeZone.getTimeZone(\""
                    + castRuleCtx.getSessionZoneId().getId()
                    + "\")";
        }

        @Override
        public String declareVariable(String type, String variablePrefix) {
            String variableName = variablePrefix + "$" + variableIndex;
            variableDeclarationStatements.add(type + " " + variableName + ";");
            variableIndex++;
            return variableName;
        }

        @Override
        public String declareTypeSerializer(LogicalType type) {
            return "this."
                    + typeSerializers
                            .computeIfAbsent(
                                    type,
                                    t -> {
                                        String term = "typeSerializer$" + variableIndex;
                                        TypeSerializer<?> serializer =
                                                InternalSerializers.create(t);
                                        this.classFields.add(
                                                "private final "
                                                        + className(serializer.getClass())
                                                        + " "
                                                        + term
                                                        + ";");
                                        this.constructorArguments.add(
                                                new SimpleImmutableEntry<>(term, serializer));

                                        variableIndex++;
                                        return new SimpleImmutableEntry<>(term, serializer);
                                    })
                            .getKey();
        }

        @Override
        public String declareDataStructureConverter(LogicalType logicalType) {
            return "this."
                    + dataStructureConverters
                            .computeIfAbsent(
                                    logicalType,
                                    t -> {
                                        String term = "dataStructureConverter$" + variableIndex;
                                        DataStructureConverter<Object, Object> converter =
                                                DataStructureConverters.getConverter(
                                                        DataTypes.of(t));
                                        this.classFields.add(
                                                "private final "
                                                        + className(converter.getClass())
                                                        + " "
                                                        + term
                                                        + ";");
                                        this.constructorArguments.add(
                                                new SimpleImmutableEntry<>(term, converter));

                                        variableIndex++;
                                        return new SimpleImmutableEntry<>(term, converter);
                                    })
                            .getKey();
        }

        @Override
        public String declareClassField(String type, String name, String initialization) {
            this.classFields.add(type + " " + name + " = " + initialization + ";");
            return "this." + name;
        }

        public List<String> getClassFields() {
            return classFields;
        }
    }
}
