package org.apache.parquet.parqour.queryinfo;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.parqour.ingest.schema.QueryInfo;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertEquals;

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

    QueryInfo queryInfo = TestTools.generateSchemaInfoFromSchema(schema);

    assertEquals(getPath(queryInfo.getColumnDescriptorByPath("first")), "first");
    assertEquals(getPath(queryInfo.getColumnDescriptorByPath("second")), "second");
    assertEquals(getPath(queryInfo.getColumnDescriptorByPath("third")), "third");
  }

  @Test
  public void testHierarchialSchema() {
    MessageType schema = new MessageType("root",
      new GroupType(REQUIRED, "subgroup",
        new PrimitiveType(REQUIRED, INT32, "first"),
        new PrimitiveType(REQUIRED, INT32, "second")),
      new PrimitiveType(REQUIRED, INT32, "thirth")
    );

    QueryInfo queryInfo = TestTools.generateSchemaInfoFromSchema(schema);

    assertEquals(getPath(queryInfo.getColumnDescriptorByPath("subgroup.first")), "subgroup.first");
    assertEquals(getPath(queryInfo.getColumnDescriptorByPath("subgroup.second")), "subgroup.second");
    assertEquals(getPath(queryInfo.getColumnDescriptorByPath("thirth")), "thirth");
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
