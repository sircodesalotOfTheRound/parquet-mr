package org.apache.parquet.parqour.ingest.read.nodes.impl;

import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.read.nodes.categories.AggregatingIngestNode;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.schema.GroupType;

/**
 * Created by sircodesalot on 6/2/15.
 */
public final class GroupIngestNode extends AggregatingIngestNode {

  public GroupIngestNode(SchemaInfo schemaInfo, AggregatingIngestNode aggregatingIngestNode, String childPath, GroupType child, DiskInterfaceManager diskInterfaceManager, int childColumnIndex) {
    super(schemaInfo, aggregatingIngestNode, childPath, child, diskInterfaceManager, childColumnIndex);
  }

}
