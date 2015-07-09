package org.apache.parquet.parqour.schema;

import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.parqour.testtools.TestTools;
import org.junit.Test;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

/**
 * Created by sircodesalot on 6/9/15.
 */
public class TestSchemaInfo {

  @Test
  public void testSingleFlatChildListSchema() {
    MessageType schema = new MessageType("root",
      new PrimitiveType(REQUIRED, INT32, "first"),
      new PrimitiveType(REQUIRED, INT32, "second"),
      new PrimitiveType(REQUIRED, INT32, "third")
    );

    SchemaInfo schemaInfo = TestTools.generateSchemaInfoFromSchema(schema);

    assertEquals(getPath(schemaInfo.getColumnDescriptorByPath("first")), "first");
    assertEquals(getPath(schemaInfo.getColumnDescriptorByPath("second")), "second");
    assertEquals(getPath(schemaInfo.getColumnDescriptorByPath("third")), "third");
  }

  @Test
  public void testHierarchialSchema() {
    MessageType schema = new MessageType("root",
      new GroupType(REQUIRED, "subgroup",
        new PrimitiveType(REQUIRED, INT32, "first"),
        new PrimitiveType(REQUIRED, INT32, "second")),
      new PrimitiveType(REQUIRED, INT32, "thirth")
    );

    SchemaInfo schemaInfo = TestTools.generateSchemaInfoFromSchema(schema);

    assertEquals(getPath(schemaInfo.getColumnDescriptorByPath("subgroup.first")), "subgroup.first");
    assertEquals(getPath(schemaInfo.getColumnDescriptorByPath("subgroup.second")), "subgroup.second");
    assertEquals(getPath(schemaInfo.getColumnDescriptorByPath("thirth")), "thirth");
  }

  private String getPath(ColumnDescriptor descriptor) {
    StringBuilder builder = new StringBuilder();
    for (String item : descriptor.getPath()) {
      if (builder.length() > 0) {
        builder.append(".");
      }

      builder.append(item);
    }

    return builder.toString();
  }

}
