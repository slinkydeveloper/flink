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

package org.apache.flink.table.data.casting.rules;

import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.casting.CastExecutor;
import org.apache.flink.table.data.casting.CastRulePredicate;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.time.ZoneId;

/**
 * Cast rule for {@link LogicalTypeFamily#TIMESTAMP} to {@link LogicalTypeFamily#CHARACTER_STRING}.
 */
public class TimestampToStringCastRule extends AbstractCharacterFamilyTargetRule<TimestampData> {

    public static final TimestampToStringCastRule INSTANCE = new TimestampToStringCastRule();

    private TimestampToStringCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeFamily.TIMESTAMP)
                        .target(LogicalTypeFamily.CHARACTER_STRING)
                        .build());
    }

    @Override
    protected CastExecutor<TimestampData, String> create(
            Context context, LogicalType inputLogicalType) {
        ZoneId zoneId =
                inputLogicalType.getTypeRoot() == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE
                        ? context.getSessionZoneId()
                        : ZoneId.of("UTC");
        // TODO use DateTimeUtils and wrap the zoneId
        return TimestampData::toString;
    }
}
