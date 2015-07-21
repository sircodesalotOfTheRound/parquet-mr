package org.apache.parquet.parqour.ingest.read.nodes.impl.field;

import org.apache.parquet.parqour.ingest.cursor.iface.AdvanceableCursor;
import org.apache.parquet.parqour.ingest.cursor.noniterable.GroupCursor;
import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.read.nodes.IngestNodeSet;
import org.apache.parquet.parqour.ingest.read.nodes.categories.AggregatingIngestNode;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.schema.GroupType;

import java.util.Arrays;

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

  protected abstract GroupCursor generateAggregateCursor(IngestNodeSet children, Integer[][] schemaLinks);

  @Override
  protected void expandIngestBuffer() {
    for (int index = 0; index < childColumnCount; index++) {
      schemaLinks[index] = Arrays.copyOf(schemaLinks[index], ingestBufferLength * 2);
    }

    this.ingestBufferLength *= 2;
    this.aggregate.setSchemaLinks(schemaLinks);
  }

  @Override
  protected AdvanceableCursor onLinkToParent(AggregatingIngestNode parentNode) {
    return aggregate;
  }

  public GroupCursor cursor() { return this.aggregate; }
}
