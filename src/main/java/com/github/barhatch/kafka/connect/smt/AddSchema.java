/*
 * Copyright © 2023 Barhatch Limited (tom@miller.mx)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.barhatch.kafka.connect.smt;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;

public abstract class AddSchema<R extends ConnectRecord<R>> implements Transformation<R> {
  static String FIELD_NAME = "field.name";
  private static final String PURPOSE = "Add schema to a record";
  static final ConfigDef CONFIG_DEF = new ConfigDef().define(FIELD_NAME, Type.STRING, Importance.HIGH, PURPOSE);
  private String fieldName;

  @Override
  public void configure(Map<String, ?> props) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    fieldName = config.getString(FIELD_NAME);
  }

  @Override
  public R apply(R record) {
    if (actualSchema(record) == null) {
      return record;
    } else {
      return applySchema(record);
    }
  }

  private R applySchema(R record) {
    final Object value = actualValue(record);

    Schema updatedSchema = makeUpdatedSchema();

    final Struct updatedValue = new Struct(updatedSchema);

    if (value != null) {
      updatedValue.put(fieldName,
          new String((byte[]) value, StandardCharsets.UTF_8).replaceAll("null", "\"\"").replaceAll("\u0000", ""));
    } else {
      updatedValue.put(fieldName, new String());
    }

    return newRecord(record, updatedSchema, updatedValue);
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
  }

  private Schema makeUpdatedSchema() {
    final SchemaBuilder builder = SchemaBuilder.struct()
        .name("json_schema")
        .field(fieldName, Schema.STRING_SCHEMA);

    return builder.build();
  }

  protected abstract Schema actualSchema(R record);

  protected abstract Object actualValue(R record);

  protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

  public static class Key<R extends ConnectRecord<R>> extends AddSchema<R> {

    @Override
    protected Schema actualSchema(R record) {
      return record.keySchema();
    }

    @Override
    protected Object actualValue(R record) {
      return record.key();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue,
          record.valueSchema(), record.value(), record.timestamp());
    }
  }

  public static class Value<R extends ConnectRecord<R>> extends AddSchema<R> {

    @Override
    protected Schema actualSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object actualValue(R record) {
      return record.value();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema,
          updatedValue, record.timestamp());
    }

  }
}
