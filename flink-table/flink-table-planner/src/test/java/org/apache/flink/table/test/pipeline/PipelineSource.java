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

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PipelineSource {

    private final String name;
    private DataType dataType;
    private final List<Row> rows;
    private ChangelogMode changelogMode;

    private PipelineSource(String name) {
        this.name = name;
        this.rows = new ArrayList<>();
        this.changelogMode = ChangelogMode.insertOnly();
    }

    public PipelineSource typed(DataType dataType) {
        this.dataType = dataType;
        return this;
    }

    public PipelineSource rows(Row... rows) {
        this.rows.addAll(Arrays.asList(rows));
        return this;
    }

    public PipelineSource rows(Collection<Row> rows) {
        this.rows.addAll(rows);
        return this;
    }

    public PipelineSource rows(Stream<Row> rows) {
        this.rows.addAll(rows.collect(Collectors.toList()));
        return this;
    }

    public PipelineSource changelogMode(ChangelogMode changelogMode) {
        this.changelogMode = changelogMode;
        return this;
    }

    public String getName() {
        return name;
    }

    public DataType getDataType() {
        if (dataType == null) {
            if (this.rows.size() == 0) {
                throw new IllegalArgumentException(
                        "You need to provide at least one row to derive the type automatically. "
                                + "Please either add a row or define the type manually.");
            }
            dataType = PipelineUtils.inferDataType(this.rows.get(0));
        }
        return dataType;
    }

    public List<Row> getRows() {
        return Collections.unmodifiableList(rows);
    }

    public ChangelogMode getChangelogMode() {
        return changelogMode;
    }

    public SourceFunction<RowData> toSourceFunction() {
        DataStructureConverter<Object, Object> dataStructureConverter =
                DataStructureConverters.getConverter(getDataType());
        return new SourceFunction<RowData>() {
            @Override
            public void run(SourceContext<RowData> ctx) {
                for (Row row : getRows()) {
                    ctx.collect((RowData) dataStructureConverter.toInternal(row));
                }
                ctx.close();
            }

            @Override
            public void cancel() {}
        };
    }

    public static PipelineSource named(String name) {
        return new PipelineSource(name);
    }
}
