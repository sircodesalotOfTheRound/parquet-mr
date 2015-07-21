package org.apache.parquet.parqour.ingest.read.nodes.impl.field;

import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.read.nodes.categories.AggregatingIngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.categories.IngestNode;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.schema.GroupType;

import java.util.Arrays;

/**
 * Created by sircodesalot on 6/2/15.
 */
public final class RepeatingGroupIngestNode extends GroupIngestNode {

  // Todo: Make these an array, not a single item.
  private int listHeaderIndex;
  //private int numberOfItemsInList;

  public RepeatingGroupIngestNode(SchemaInfo schemaInfo, AggregatingIngestNode aggregatingIngestNode, String childPath, GroupType child, DiskInterfaceManager diskInterfaceManager, int childColumnIndex) {
    super(schemaInfo, aggregatingIngestNode, childPath, child, diskInterfaceManager, childColumnIndex);

    this.listHeaderIndex = -1;
  }

  @Override
  public final void linkSchema(IngestNode child) {
    int childColumnIndex = child.columnIndex();
    int schemaLinkWriteIndex = schemaLinkWriteIndexForColumn[childColumnIndex];

    if (currentRowNumber != child.currentRowNumber()) {
      this.currentRowNumber = child.currentRowNumber();
      this.listHeaderIndex = -1;

      Arrays.fill(schemaLinkWriteIndexForColumn, 0);
      schemaLinkWriteIndex = 0;
    }

    this.currentEntryRepetitionLevel = child.currentEntryRepetitionLevel();
    this.currentEntryDefinitionLevel = child.currentEntryDefinitionLevel();

    boolean childLinkIsDefined = currentEntryDefinitionLevel >= child.nodeDefinitionLevel();
    boolean requiresNewList = currentEntryRepetitionLevel < repetitionLevelAtThisNode;

    if (requiresNewList) {
      this.listHeaderIndex = schemaLinkWriteIndex++;
      this.currentLinkSiteIndex = listHeaderIndex;

      schemaLinks[childColumnIndex][listHeaderIndex] = 0;

      if (child.isSchemaReportingNode()) {
        parent.linkSchema(this);
      }
    }

    if (childLinkIsDefined) {
      this.schemaLinks[childColumnIndex][schemaLinkWriteIndex++] = child.currentLinkSiteIndex();
    } else {
      this.schemaLinks[childColumnIndex][schemaLinkWriteIndex++] = null;
    }

    schemaLinks[childColumnIndex][listHeaderIndex]++;
    this.schemaLinkWriteIndexForColumn[childColumnIndex] = schemaLinkWriteIndex;
  }
}
