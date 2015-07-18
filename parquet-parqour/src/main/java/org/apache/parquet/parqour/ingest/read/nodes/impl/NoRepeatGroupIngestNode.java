package org.apache.parquet.parqour.ingest.read.nodes.impl;

import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.read.nodes.categories.AggregatingIngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.categories.IngestNode;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.schema.GroupType;

import java.util.Arrays;

/**
 * Created by sircodesalot on 6/2/15.
 */
public final class NoRepeatGroupIngestNode extends AggregatingIngestNode {

  public NoRepeatGroupIngestNode(SchemaInfo schemaInfo, AggregatingIngestNode aggregatingIngestNode, String childPath, GroupType child, DiskInterfaceManager diskInterfaceManager, int childColumnIndex) {
    super(schemaInfo, aggregatingIngestNode, childPath, child, diskInterfaceManager, childColumnIndex);
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

    this.currentEntryRepetitionLevel = child.currentEntryRepetitionLevel();
    this.currentEntryDefinitionLevel = child.currentEntryDefinitionLevel();
    this.currentLinkSiteIndex = schemaLinkWriteIndex;

    if (currentEntryDefinitionLevel >= child.nodeDefinitionLevel()) {
      schemaLinks[childColumnIndex][schemaLinkWriteIndex++] = child.currentLinkSiteIndex();
    } else {
      schemaLinks[childColumnIndex][schemaLinkWriteIndex++] = null;
    }

    // If this node reports schema, then continue upstream:
    if (child.isSchemaReportingNode()) {
      parent.linkSchema(this);
    }

    schemaLinkWriteIndexForColumn[childColumnIndex] = schemaLinkWriteIndex;
  }

  @Override
  public void finishRow(IngestNode child) {
    // If this node reports schema, then continue upstream:
    if (child.isSchemaReportingNode()) {
      parent.finishRow(this);
    }
  }
}
