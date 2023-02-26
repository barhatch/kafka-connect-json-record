package com.github.barhatch.kafka.connect.smt;

import static org.junit.Assert.assertEquals;

import com.github.barhatch.kafka.connect.smt.AddSchema.Value;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AddSchemaTest {
  private final AddSchema<SourceRecord> transform = new Value<>();

  @Before
  public void setUp() {
    final Map<String, Object> props = new HashMap<>();
    props.put(AddSchema.FIELD_NAME, "name");
    transform.configure(props);
  }

  @After
  public void tearDown() {
    transform.close();
  }

  @Test
  public void testWithoutSchema() {
    String test = "Hello World!";
    final SourceRecord record = new SourceRecord(null, null, "topic", null,
        test);
    final SourceRecord transformedRecord = transform.apply(record);
    assertEquals(test, transformedRecord.value());
  }

  @Test
  public void testWithSchema() {
    String test = "{\"account\":{\"id\":\"cabb4e2a-be27-3863-b3f9-d2c6ecd835db\",\"opening_timestamp\":null}}";
    final Schema bytesSchema = SchemaBuilder.bytes().name("testSchema").build();
    final byte[] bytes = test.getBytes();
    final SourceRecord record = new SourceRecord(null, null, "topic", bytesSchema, bytes);
    final SourceRecord transformedRecord = transform.apply(record);
    assertEquals("{\"account\":{\"id\":\"cabb4e2a-be27-3863-b3f9-d2c6ecd835db\",\"opening_timestamp\":\"\"}}",
        ((Struct) transformedRecord.value()).getString("name"));
  }

  @Test
  public void testWithNull() {
    String test = null;
    final Schema bytesSchema = SchemaBuilder.bytes().name("testSchema").build();
    final SourceRecord record = new SourceRecord(null, null, "topic", bytesSchema, test);
    final SourceRecord transformedRecord = transform.apply(record);
    assertEquals(new String(), ((Struct) transformedRecord.value()).getString("name"));
  }
}
