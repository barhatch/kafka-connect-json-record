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

import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AddSchema<R extends ConnectRecord<R>> implements Transformation<R> {

  private static Logger logger = LoggerFactory.getLogger(AddSchema.class);

  public static final String OVERVIEW_DOC = "Add schema to a raw JSON connect record";

  private interface ConfigName {
    String JSON_STRING_FIELD_NAME = "json.string.field.name";
    String JSON_WRITER_OUTPUT_MODE = "json.writer.output.mode";
  }

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(ConfigName.JSON_STRING_FIELD_NAME, ConfigDef.Type.STRING, "record", ConfigDef.Importance.HIGH,
          "Field name for JSON record")
      .define(ConfigName.JSON_WRITER_OUTPUT_MODE, ConfigDef.Type.STRING, "RELAXED", ConfigDef.Importance.MEDIUM,
          "Output mode of JSON Writer (RELAXED,EXTENDED,SHELL or STRICT)");

  private static final String PURPOSE = "adding Schema to schemaless record";

  private String fieldName;
  private Schema jsonStringOutputSchema;
  JsonWriterSettings jsonWriterSettings;

  @Override
  public void configure(Map<String, ?> props) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    fieldName = config.getString(ConfigName.JSON_STRING_FIELD_NAME);
    jsonStringOutputSchema = makeUpdatedSchema();

    jsonWriterSettings = JsonWriterSettings
        .builder()
        .outputMode(toJsonMode(config.getString(ConfigName.JSON_WRITER_OUTPUT_MODE)))
        .build();
  }

  @Override
  public R apply(R record) {
    if (isTombstoneRecord(record))
      return record;

    if (operatingSchema(record) == null) {
      return applySchemaless(record);
    } else {
      logger.info("addSchema is ignoring value/key with schema");
      return record;
    }
  }

  private R applySchemaless(R record) {
    final Object value = operatingValue(record);

    ObjectMapper objectMapper = new ObjectMapper();

    String valueAsString = null;

    try {
      valueAsString = objectMapper.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      // catch various errors
      e.printStackTrace();
      logger.info("failed to convert value to JSON string");
    }

    final Struct jsonStringOutputStruct = new Struct(jsonStringOutputSchema);

    jsonStringOutputStruct.put(fieldName, valueAsString);

    return newRecord(record, jsonStringOutputSchema, jsonStringOutputStruct);
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
  }

  private Schema makeUpdatedSchema() {
    return SchemaBuilder
        .struct()
        .name("jsonStringSchema")
        .version(1)
        .field(fieldName, Schema.STRING_SCHEMA)
        .build();
  }

  private JsonMode toJsonMode(String jsonMode) {
    switch (jsonMode) {
      case "SHELL":
        return JsonMode.SHELL;
      case "EXTENDED":
        return JsonMode.EXTENDED;
      case "STRICT":
        return JsonMode.STRICT;
      default:
        return JsonMode.RELAXED;
    }
  }

  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

  protected abstract boolean isTombstoneRecord(R record);

  public static class Key<R extends ConnectRecord<R>> extends AddSchema<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.keySchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.key();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue,
          record.valueSchema(), record.value(), record.timestamp());
    }

    @Override
    protected boolean isTombstoneRecord(R record) {
      return record.key() == null;
    }

  }

  public static class Value<R extends ConnectRecord<R>> extends AddSchema<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.value();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema,
          updatedValue, record.timestamp());
    }

    @Override
    protected boolean isTombstoneRecord(R record) {
      return record.value() == null;
    }

  }
}
