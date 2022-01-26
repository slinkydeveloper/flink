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

package org.apache.flink.table.test.pipeline;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.ClassDataTypeConverter;
import org.apache.flink.types.Row;

/**
 * Utilities to deal with pipeline interfaces. Nothing should be exposed here, but rather in the
 * interfaces/POJOs directly.
 */
class PipelineUtils {

    private PipelineUtils() {}

    static DataType inferDataType(Row row) {
        DataTypes.Field[] fields = new DataTypes.Field[row.getArity()];

        for (int i = 0; i < row.getArity(); i++) {
            Object value = row.getField(i);
            if (value == null) {
                throw new IllegalArgumentException(
                        "DataType inference doesn't support null values. Please define the type manually");
            }

            fields[i] =
                    DataTypes.FIELD(
                            "f" + i,
                            ClassDataTypeConverter.extractDataType(value.getClass())
                                    .orElseThrow(
                                            () ->
                                                    new IllegalArgumentException(
                                                            "Cannot infer the DataType automatically. Please define the type manually")));
        }

        return DataTypes.ROW(fields);
    }
}
