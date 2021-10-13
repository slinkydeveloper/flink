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

package org.apache.flink.table.data.casting;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;

/**
 * Interface to model a function that performs the casting of one value to another type.
 *
 * <p>This interface is serializable in order to be embedded in the runtime codegen.
 *
 * @param <IN> Input internal type
 * @param <OUT> Output internal type
 */
@Internal
@FunctionalInterface
public interface CastExecutor<IN, OUT> extends java.io.Serializable {
    /** Cast the input value. */
    OUT cast(IN value) throws TableException;
}
