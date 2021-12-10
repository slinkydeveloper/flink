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
import org.apache.flink.connector.pulsar.SampleMessage.SubMessage.NestedMessage;
import org.apache.flink.connector.pulsar.SampleMessage.TestMessage;
import org.apache.flink.connector.pulsar.testutils.SampleData.Bar;
import org.apache.flink.connector.pulsar.testutils.SampleData.FL;
import org.apache.flink.connector.pulsar.testutils.SampleData.Foo;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link PulsarSchemaUtils}. */
class PulsarSchemaUtilsTest {

    @Test
    void haveProtobufShouldReturnTrueIfWeProvidedIt() {
        assertThat(PulsarSchemaUtils.haveProtobuf()).isTrue();
    }

    @Test
    void protobufClassValidation() {
        assertThat(PulsarSchemaUtils.isProtobufTypeClass(TestMessage.class)).isTrue();
        assertThat(PulsarSchemaUtils.isProtobufTypeClass(SubMessage.class)).isTrue();
        assertThat(PulsarSchemaUtils.isProtobufTypeClass(NestedMessage.class)).isTrue();

        assertThat(PulsarSchemaUtils.isProtobufTypeClass(Bar.class)).isFalse();
        assertThat(PulsarSchemaUtils.isProtobufTypeClass(FL.class)).isFalse();
        assertThat(PulsarSchemaUtils.isProtobufTypeClass(Foo.class)).isFalse();
    }

    @Test
    @SuppressWarnings("java:S5778")
    void createSchemaForComplexSchema() {
        // Avro
        Schema<Foo> avro1 = Schema.AVRO(Foo.class);
        PulsarSchema<Foo> avro2 = new PulsarSchema<>(avro1, Foo.class);
        assertThatThrownBy(() -> PulsarSchemaUtils.createSchema(avro1.getSchemaInfo()))
                .isInstanceOf(NullPointerException.class);

        Schema<Foo> schema = PulsarSchemaUtils.createSchema(avro2.getSchemaInfo());
        assertThat(avro1.getSchemaInfo()).isEqualTo(schema.getSchemaInfo());
        assertThat(avro2.getSchemaInfo()).isEqualTo(schema.getSchemaInfo());

        // JSON
        Schema<FL> json1 = Schema.JSON(FL.class);
        PulsarSchema<FL> json2 = new PulsarSchema<>(json1, FL.class);
        Schema<FL> json3 = PulsarSchemaUtils.createSchema(json2.getSchemaInfo());
        assertThat(json1.getSchemaInfo()).isEqualTo(json3.getSchemaInfo());
        assertThat(json2.getSchemaInfo()).isEqualTo(json3.getSchemaInfo());

        // Protobuf Native
        Schema<TestMessage> proto1 = Schema.PROTOBUF_NATIVE(TestMessage.class);
        PulsarSchema<TestMessage> proto2 = new PulsarSchema<>(proto1, TestMessage.class);
        Schema<TestMessage> proto3 = PulsarSchemaUtils.createSchema(proto2.getSchemaInfo());
        assertThat(proto1.getSchemaInfo()).isEqualTo(proto3.getSchemaInfo());
        assertThat(proto2.getSchemaInfo()).isEqualTo(proto3.getSchemaInfo());

        // KeyValue
        Schema<KeyValue<byte[], byte[]>> kvBytes1 = Schema.KV_BYTES();
        PulsarSchema<KeyValue<byte[], byte[]>> kvBytes2 =
                new PulsarSchema<>(kvBytes1, byte[].class, byte[].class);
        Schema<KeyValue<byte[], byte[]>> kvBytes3 =
                PulsarSchemaUtils.createSchema(kvBytes2.getSchemaInfo());
        assertThat(kvBytes1.getSchemaInfo()).isEqualTo(kvBytes3.getSchemaInfo());
    }

    @Test
    void encodeAndDecodeClassInfo() {
        Schema<Foo> schema = Schema.AVRO(Foo.class);
        SchemaInfo info = schema.getSchemaInfo();
        SchemaInfo newInfo = PulsarSchemaUtils.encodeClassInfo(info, Foo.class);
        assertThatThrownBy(() -> PulsarSchemaUtils.decodeClassInfo(newInfo)).isNull();

        Class<Foo> clazz = PulsarSchemaUtils.decodeClassInfo(newInfo);
        assertThat(Foo.class).isEqualTo(clazz);
    }
}
