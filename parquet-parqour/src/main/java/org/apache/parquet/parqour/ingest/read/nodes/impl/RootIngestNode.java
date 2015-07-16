package org.apache.parquet.parqour.ingest.read.nodes.impl;

import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.read.nodes.categories.AggregatingIngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.categories.IngestNode;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;

import java.util.Arrays;

/**
 * Created by sircodesalot on 6/2/15.
 */
public final class RootIngestNode extends AggregatingIngestNode {
  public RootIngestNode(SchemaInfo schemaInfo, DiskInterfaceManager diskInterfaceManager) {
    super(schemaInfo, schemaInfo.metadata().getFileMetaData().getSchema(), diskInterfaceManager);
  }

  @Override
  public final void linkSchema(IngestNode child) {
    int childColumnIndex = child.columnIndex();
    int schemaLinkWriteIndex = schemaLinkWriteIndexForColumn[childColumnIndex];

    if (currentRowNumber != child.currentRowNumber()) {
      currentRowNumber = child.currentRowNumber();

      Arrays.fill(schemaLinkWriteIndexForColumn, 0);
      schemaLinkWriteIndex = 0;
    }

    if (child.currentEntryDefinitionLevel() >= child.nodeDefinitionLevel()) {
      schemaLinks[childColumnIndex][schemaLinkWriteIndex++] = child.currentLinkSiteIndex();
    } else {
      schemaLinks[childColumnIndex][schemaLinkWriteIndex++] = null;
    }

    this.schemaLinkWriteIndexForColumn[childColumnIndex] = schemaLinkWriteIndex;
  }

  @Override
  public void finishRow(IngestNode child) {
    /* NO-OP */
  }

}
