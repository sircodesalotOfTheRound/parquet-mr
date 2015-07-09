package org.apache.parquet.parqour.ingest.read.nodes.impl;

import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.read.nodes.categories.AggregatingIngestNode;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;

/**
 * Created by sircodesalot on 6/2/15.
 */
public final class RootIngestNode extends AggregatingIngestNode {
  public RootIngestNode(SchemaInfo schemaInfo, DiskInterfaceManager diskInterfaceManager) {
    super(schemaInfo, schemaInfo.metadata().getFileMetaData().getSchema(), diskInterfaceManager);
  }
}
