package org.apache.parquet.parqour.ingest.tree.nodes;

import org.apache.parquet.parqour.ingest.cursor.iface.AdvanceableCursor;
import org.apache.parquet.parqour.ingest.read.nodes.categories.AggregatingIngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.categories.IngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.categories.IngestNodeCategory;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.parqour.testtools.TestTools;
import org.junit.Test;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

/**
 * Created by sircodesalot on 6/19/15.
 */
public class TestIngestNodeInfo {
  private static final GroupType SCHEMA = new GroupType(REQUIRED, "schema",
    new PrimitiveType(REQUIRED, INT32, "primitive"));

  private static final SchemaInfo SCHEMA_INFO = TestTools.generateSchemaInfoFromSchema(SCHEMA);
  private static final AggregatingIngestNode AGGREGATOR = mockAggregationNode();

  @Test
  public void testBasicNodeInfo() {
    GroupType schema = new GroupType(REQUIRED, "schema",
      new PrimitiveType(REQUIRED, INT32, "primitive"));

    IngestNode node = new SimpleMockIngestNode(SCHEMA_INFO, AGGREGATOR, "primitive", schema.getType("primitive"), IngestNodeCategory.DATA_INGEST, 0);

    assertEquals("primitive", node.name());
    assertTrue(node.canPerformTrueFastForwards());
  }

  @Test
  public void testCanPerformFastForwards() {

  }

  public static AggregatingIngestNode mockAggregationNode() {
    AggregatingIngestNode aggregatingIngestNode = mock(AggregatingIngestNode.class);
    when(aggregatingIngestNode.schemaNode()).thenReturn(SCHEMA);


    return aggregatingIngestNode;
  }

  public static class SimpleMockIngestNode extends IngestNode {
    public SimpleMockIngestNode(SchemaInfo schemaInfo, AggregatingIngestNode parent, String path, Type schemaNode, IngestNodeCategory category, int childNodeIndex) {
      super(schemaInfo, parent, path, schemaNode, category, childNodeIndex);
    }

    @Override
    protected AdvanceableCursor onLinkToParent(AggregatingIngestNode parentNode, Integer[] relationships) {
      return mock(AdvanceableCursor.class);
    }
  }
}
