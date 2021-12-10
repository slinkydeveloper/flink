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

package org.apache.flink.connector.file.table;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link JoinedRowData}. */
public class EnrichedRowDataTest {

    @Test
    public void testEnrichedRow() {
        final List<String> completeRowFields =
                Arrays.asList(
                        "fixedRow1",
                        "mutableRow1",
                        "mutableRow3",
                        "mutableRow2",
                        "fixedRow2",
                        "mutableRow4");
        final List<String> mutableRowFields =
                Arrays.asList("mutableRow1", "mutableRow2", "mutableRow3", "mutableRow4");
        final List<String> fixedRowFields = Arrays.asList("fixedRow1", "fixedRow2");

        final RowData fixedRowData = GenericRowData.of(1L, 2L);
        final EnrichedRowData enrichedRowData =
                EnrichedRowData.from(
                        fixedRowData, completeRowFields, mutableRowFields, fixedRowFields);
        final RowData mutableRowData = GenericRowData.of(3L, 4L, 5L, 6L);
        enrichedRowData.replaceMutableRow(mutableRowData);

        assertThat(enrichedRowData.getRowKind()).isEqualTo(RowKind.INSERT);
        assertThat(enrichedRowData.getArity()).isEqualTo(6);
        assertThat(enrichedRowData.getLong(0)).isEqualTo(1L);
        assertThat(enrichedRowData.getLong(1)).isEqualTo(3L);
        assertThat(enrichedRowData.getLong(2)).isEqualTo(5L);
        assertThat(enrichedRowData.getLong(3)).isEqualTo(4L);
        assertThat(enrichedRowData.getLong(4)).isEqualTo(2L);
        assertThat(enrichedRowData.getLong(5)).isEqualTo(6L);

        final RowData newMutableRowData = GenericRowData.of(7L, 8L, 9L, 10L);
        enrichedRowData.replaceMutableRow(newMutableRowData);

        assertThat(enrichedRowData.getLong(0)).isEqualTo(1L);
        assertThat(enrichedRowData.getLong(1)).isEqualTo(7L);
        assertThat(enrichedRowData.getLong(2)).isEqualTo(9L);
        assertThat(enrichedRowData.getLong(3)).isEqualTo(8L);
        assertThat(enrichedRowData.getLong(4)).isEqualTo(2L);
        assertThat(enrichedRowData.getLong(5)).isEqualTo(10L);
    }
}
