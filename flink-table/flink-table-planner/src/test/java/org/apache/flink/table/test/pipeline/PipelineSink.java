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

import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PipelineSink {

    private final String name;
    private final DataType dataType;
    private final List<Row> rows;

    private PipelineSink(String name, DataType dataType, List<Row> rows) {
        this.name = name;
        this.dataType = dataType;
        this.rows = rows;
    }

    public String getName() {
        return name;
    }

    public DataType getDataType() {
        return dataType;
    }

    public List<Row> getRows() {
        return rows;
    }

    public static Builder named(String name) {
        return new Builder(name);
    }

    public static class Builder {

        private final String name;
        private DataType dataType;
        private final List<Row> rows = new ArrayList<>();

        public Builder(String name) {
            this.name = name;
        }

        public Builder type(DataType dataType) {
            this.dataType = dataType;
            return this;
        }

        public Builder rows(Row... rows) {
            this.rows.addAll(Arrays.asList(rows));
            return this;
        }

        public Builder rows(Collection<Row> rows) {
            this.rows.addAll(rows);
            return this;
        }

        public Builder rows(Stream<Row> rows) {
            this.rows.addAll(rows.collect(Collectors.toList()));
            return this;
        }

        public PipelineSink build() {
            if (dataType == null) {
                if (this.rows.size() == 0) {
                    throw new IllegalArgumentException(
                            "You need to have at least one row to derive the type automatically. Please either add a row or define the type manually.");
                }
                dataType = PipelineUtils.inferDataType(this.rows.get(0));
            }
            return new PipelineSink(name, dataType, rows);
        }
    }
}
