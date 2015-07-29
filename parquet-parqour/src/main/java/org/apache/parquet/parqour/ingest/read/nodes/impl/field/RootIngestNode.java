package org.apache.parquet.parqour.ingest.read.nodes.impl.field;

import org.apache.parquet.parqour.ingest.cursor.iface.AdvanceableCursor;
import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.cursor.implementations.noniterable.root.RootCursor;
import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.read.nodes.categories.AggregatingIngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.categories.IngestNode;
import org.apache.parquet.parqour.ingest.schema.QueryInfo;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Created by sircodesalot on 6/2/15.
 */
public final class RootIngestNode extends AggregatingIngestNode {
  private final Integer[] schemaLinks;
  private final RootCursor cursor;

  public RootIngestNode(QueryInfo queryInfo, DiskInterfaceManager diskInterfaceManager) {
    super(queryInfo, queryInfo.metadata().getFileMetaData().getSchema(), diskInterfaceManager);

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
  protected AdvanceableCursor onLinkToParent(AggregatingIngestNode parentNode) {
    throw new NotImplementedException();
  }

  @Override
  protected void expandIngestBuffer() {
    throw new NotImplementedException();
  }
}
