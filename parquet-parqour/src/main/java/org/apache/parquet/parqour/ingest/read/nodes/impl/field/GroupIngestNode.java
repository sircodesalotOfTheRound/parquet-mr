package org.apache.parquet.parqour.ingest.read.nodes.impl.field;

import org.apache.parquet.parqour.ingest.cursor.noniterable.GroupCursor;
import org.apache.parquet.parqour.ingest.cursor.iface.AdvanceableCursor;
import org.apache.parquet.parqour.ingest.cursor.iterable.field.GroupIterableCursor;
import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.read.nodes.IngestNodeSet;
import org.apache.parquet.parqour.ingest.read.nodes.categories.AggregatingIngestNode;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

/**
 * Created by sircodesalot on 7/20/15.
 */
public abstract class GroupIngestNode extends AggregatingIngestNode {
  protected final GroupCursor aggregate;
  protected Integer[][] schemaLinks;

  public GroupIngestNode(SchemaInfo schemaInfo, AggregatingIngestNode parent,
                         String fqn, GroupType groupSchema,
                         DiskInterfaceManager diskInterfaceManager, int childNodeIndex) {
    super(schemaInfo, parent, fqn, groupSchema, diskInterfaceManager, childNodeIndex);

    this.schemaLinks = generateSchemaLinks(childColumnCount, ingestBufferLength );
    this.aggregate = generateAggregateCursor(children, schemaLinks);
  }


  private Integer[][] generateSchemaLinks(int childColumnCount, int ingestBufferLength) {
    return new Integer[childColumnCount][ingestBufferLength];
  }

  private GroupCursor generateAggregateCursor(IngestNodeSet children, Integer[][] schemaLinks) {
    AdvanceableCursor[] childCursors = linkChildren(children);

    // The root node is a special case because it's always defined as REPEAT even though we treat it as REQUIRED.
    GroupCursor aggregate;
    if (this.hasParent && this.repetitionType == Type.Repetition.REPEATED) {
      aggregate = new GroupIterableCursor(name, columnIndex, childCursors, schemaLinks);
    }  else {
      aggregate = new GroupCursor(name, columnIndex, childCursors, schemaLinks);
    }

    return aggregate;
  }

  @Override
  protected void expandIngestBuffer() {
    int newIngestBufferLength = this.ingestBufferLength * 2;

    for (int index = 0; index < childColumnCount; index++) {
      Integer[] newChildColumnIngestBuffer = new Integer[newIngestBufferLength];
      System.arraycopy(schemaLinks[index], 0, newChildColumnIngestBuffer, 0, ingestBufferLength);

      this.schemaLinks[index] = newChildColumnIngestBuffer;
    }

    this.ingestBufferLength = newIngestBufferLength;
    this.aggregate.setSchemaLinks(schemaLinks);
  }

  @Override
  protected AdvanceableCursor onLinkToParent(AggregatingIngestNode parentNode) {
    return aggregate;
  }

  public GroupCursor cursor() { return this.aggregate; }
}
