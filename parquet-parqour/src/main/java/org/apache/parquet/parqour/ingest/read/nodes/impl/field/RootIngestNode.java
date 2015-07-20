package org.apache.parquet.parqour.ingest.read.nodes.impl.field;

import org.apache.parquet.parqour.ingest.cursor.iface.AdvanceableCursor;
import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.cursor.noniterable.RootCursor;
import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.read.nodes.categories.AggregatingIngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.categories.IngestNode;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;

import java.util.Arrays;

/**
 * Created by sircodesalot on 6/2/15.
 */
public final class RootIngestNode extends AggregatingIngestNode {
  private final Integer[] schemaLinks;
  private final RootCursor cursor;

  public RootIngestNode(SchemaInfo schemaInfo, DiskInterfaceManager diskInterfaceManager) {
    super(schemaInfo, schemaInfo.metadata().getFileMetaData().getSchema(), diskInterfaceManager);

    this.schemaLinks = new Integer[childColumnCount];
    this.cursor = generateCursor();
  }

  private RootCursor generateCursor() {
    AdvanceableCursor[] childCursors = super.linkChildren(children);
    return new RootCursor(childCursors, schemaLinks);
  }

  @Override
  public final void linkSchema(IngestNode child) {
    if (child.currentEntryDefinitionLevel() >= child.nodeDefinitionLevel()) {
      schemaLinks[child.columnIndex()] = child.currentLinkSiteIndex();
    } else {
      schemaLinks[child.columnIndex()] = null;
    }
  }

  public final Cursor cursor() { return this.cursor; }

  @Override
  public void finishRow(IngestNode child) {
    /* NO-OP */
  }

  @Override
  protected AdvanceableCursor onLinkToParent(AggregatingIngestNode parentNode) {
    return null;
  }

  @Override
  protected void expandIngestBuffer() {
    /* NO-OP */
  }
}
