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

package org.apache.flink.connector.pulsar.common.schema;

import org.apache.flink.connector.pulsar.SampleMessage.SubMessage;
import org.apache.flink.connector.pulsar.SampleMessage.TestMessage;
import org.apache.flink.connector.pulsar.testutils.SampleData.Bar;
import org.apache.flink.connector.pulsar.testutils.SampleData.FA;
import org.apache.flink.connector.pulsar.testutils.SampleData.FL;
import org.apache.flink.connector.pulsar.testutils.SampleData.Foo;
import org.apache.flink.util.InstantiationUtil;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaImpl;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchema;
import org.apache.pulsar.client.impl.schema.ProtobufSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link PulsarSchema}. */
class PulsarSchemaTest {

    private static final JSONSchema<FL> JSON = JSONSchema.of(FL.class);
    private static final AvroSchema<Bar> AVRO = AvroSchema.of(Bar.class);
    private static final ProtobufSchema<TestMessage> PROTO = ProtobufSchema.of(TestMessage.class);
    private static final ProtobufNativeSchema<SubMessage> PROTO_N =
            ProtobufNativeSchema.of(SubMessage.class);
    private static final Schema<KeyValue<Foo, FA>> KV =
            KeyValueSchemaImpl.of(Foo.class, FA.class, SchemaType.JSON);

    @Test
    void pulsarSchemaCreation() {
        assertThatThrownBy(() -> new PulsarSchema<>(Schema.BYTES)).isNull();
        assertThatThrownBy(() -> new PulsarSchema<>(Schema.STRING)).isNull();
        assertThatThrownBy(() -> new PulsarSchema<>(Schema.INT8)).isNull();
        assertThatThrownBy(() -> new PulsarSchema<>(Schema.INT16)).isNull();
        assertThatThrownBy(() -> new PulsarSchema<>(Schema.INT32)).isNull();
        assertThatThrownBy(() -> new PulsarSchema<>(Schema.INT64)).isNull();
        assertThatThrownBy(() -> new PulsarSchema<>(Schema.BOOL)).isNull();
        assertThatThrownBy(() -> new PulsarSchema<>(Schema.FLOAT)).isNull();
        assertThatThrownBy(() -> new PulsarSchema<>(Schema.DOUBLE)).isNull();
        assertThatThrownBy(() -> new PulsarSchema<>(Schema.DATE)).isNull();
        assertThatThrownBy(() -> new PulsarSchema<>(Schema.TIME)).isNull();
        assertThatThrownBy(() -> new PulsarSchema<>(Schema.TIMESTAMP)).isNull();
        assertThatThrownBy(() -> new PulsarSchema<>(Schema.INSTANT)).isNull();
        assertThatThrownBy(() -> new PulsarSchema<>(Schema.LOCAL_DATE)).isNull();
        assertThatThrownBy(() -> new PulsarSchema<>(Schema.LOCAL_TIME)).isNull();
        assertThatThrownBy(() -> new PulsarSchema<>(Schema.LOCAL_DATE_TIME)).isNull();

        assertThatThrownBy(() -> new PulsarSchema<>(JSON, FL.class)).isNull();
        assertThatThrownBy(() -> new PulsarSchema<>(AVRO, Bar.class)).isNull();
        assertThatThrownBy(() -> new PulsarSchema<>(PROTO, TestMessage.class)).isNull();
        assertThatThrownBy(() -> new PulsarSchema<>(PROTO_N, SubMessage.class)).isNull();
        assertThatThrownBy(() -> new PulsarSchema<>(KV, Foo.class, FA.class)).isNull();
    }

    @Test
    void invalidPulsarSchemaCreationWithoutClassType() {
        assertThatThrownBy(() -> new PulsarSchema<>(AVRO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PulsarSchema<>(JSON))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PulsarSchema<>(PROTO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PulsarSchema<>(PROTO_N))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PulsarSchema<>(KV))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PulsarSchema(KV, KeyValue.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void pulsarSchemaSerialization() throws Exception {
        assertPulsarSchemaIsSerializable(new PulsarSchema<>(Schema.BYTES));
        assertPulsarSchemaIsSerializable(new PulsarSchema<>(Schema.STRING));
        assertPulsarSchemaIsSerializable(new PulsarSchema<>(Schema.INT8));
        assertPulsarSchemaIsSerializable(new PulsarSchema<>(Schema.INT16));
        assertPulsarSchemaIsSerializable(new PulsarSchema<>(Schema.INT32));
        assertPulsarSchemaIsSerializable(new PulsarSchema<>(Schema.INT64));
        assertPulsarSchemaIsSerializable(new PulsarSchema<>(Schema.BOOL));
        assertPulsarSchemaIsSerializable(new PulsarSchema<>(Schema.FLOAT));
        assertPulsarSchemaIsSerializable(new PulsarSchema<>(Schema.DOUBLE));
        assertPulsarSchemaIsSerializable(new PulsarSchema<>(Schema.DATE));
        assertPulsarSchemaIsSerializable(new PulsarSchema<>(Schema.TIME));
        assertPulsarSchemaIsSerializable(new PulsarSchema<>(Schema.TIMESTAMP));
        assertPulsarSchemaIsSerializable(new PulsarSchema<>(Schema.INSTANT));
        assertPulsarSchemaIsSerializable(new PulsarSchema<>(Schema.LOCAL_DATE));
        assertPulsarSchemaIsSerializable(new PulsarSchema<>(Schema.LOCAL_TIME));
        assertPulsarSchemaIsSerializable(new PulsarSchema<>(Schema.LOCAL_DATE_TIME));
        assertPulsarSchemaIsSerializable(new PulsarSchema<>(JSON, FL.class));
        assertPulsarSchemaIsSerializable(new PulsarSchema<>(AVRO, Bar.class));
        assertPulsarSchemaIsSerializable(new PulsarSchema<>(PROTO, TestMessage.class));
        assertPulsarSchemaIsSerializable(new PulsarSchema<>(PROTO_N, SubMessage.class));
        assertPulsarSchemaIsSerializable(new PulsarSchema<>(KV, Foo.class, FA.class));
    }

    private <T> void assertPulsarSchemaIsSerializable(PulsarSchema<T> schema) throws Exception {
        PulsarSchema<T> clonedSchema = InstantiationUtil.clone(schema);
        assertThat(schema.getSchemaInfo()).isEqualTo(clonedSchema.getSchemaInfo());
        assertThat(schema.getRecordClass()).isEqualTo(clonedSchema.getRecordClass());
    }
}
