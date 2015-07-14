package org.apache.parquet.parqour.ingest.tree;

import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.parqour.ingest.read.nodes.categories.IngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.impl.NoRepeatGroupIngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.impl.i32.Int32NoRepeatIngestNode;
import org.apache.parquet.parqour.testtools.TestTools;
import org.junit.Test;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;

import static org.junit.Assert.assertEquals;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

/**
 * Created by sircodesalot on 6/9/15.
 */
public class ReadTreeTests {
  @Test
  public void simpleNodeTree() {
    GroupType schema = new GroupType(REQUIRED, "root",
      new PrimitiveType(REQUIRED, BINARY, "first"),
      new PrimitiveType(REQUIRED, BINARY, "second"),
      new GroupType(REQUIRED, "grouping",
        new PrimitiveType(REQUIRED, BINARY, "first_subnode"),
        new PrimitiveType(REQUIRED, BINARY, "second_subnode"))
    );

    IngestTree tree = TestTools.generateIngestTreeFromSchema(schema);

    IngestNode first = tree.getIngestNodeByPath("first");
    IngestNode second = tree.getIngestNodeByPath("second");
    IngestNode grouping = tree.getIngestNodeByPath("grouping");
    IngestNode firstSubnode = tree.getIngestNodeByPath("grouping.first_subnode");
    IngestNode secondSubnode = tree.getIngestNodeByPath("grouping.second_subnode");

    assertEquals(first.getClass(), Int32NoRepeatIngestNode.class);
    assertEquals(second.getClass(), Int32NoRepeatIngestNode.class);
    assertEquals(grouping.getClass(), NoRepeatGroupIngestNode.class);
    assertEquals(firstSubnode.getClass(), Int32NoRepeatIngestNode.class);
    assertEquals(secondSubnode.getClass(), Int32NoRepeatIngestNode.class);
  }
}
