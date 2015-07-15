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
public final class RepeatingGroupIngestNode extends AggregatingIngestNode {

  // Todo: Make these an array, not a single item.
  private int listHeaderIndex;
  private int numberOfItemsInList;

  public RepeatingGroupIngestNode(SchemaInfo schemaInfo, AggregatingIngestNode aggregatingIngestNode, String childPath, GroupType child, DiskInterfaceManager diskInterfaceManager, int childColumnIndex) {
    super(schemaInfo, aggregatingIngestNode, childPath, child, diskInterfaceManager, childColumnIndex);

    this.listHeaderIndex = -1;
    this.numberOfItemsInList = 0;
  }

  @Override
  public final void linkSchema(IngestNode child) {
    int childColumnIndex = child.columnIndex();
    int schemaLinkWriteIndex = schemaLinkWriteIndexForColumn[childColumnIndex];

    if (currentRowNumber != child.currentRowNumber()) {
      this.currentRowNumber = child.currentRowNumber();
      this.listHeaderIndex = -1;
      this.numberOfItemsInList = 0;

      Arrays.fill(schemaLinkWriteIndexForColumn, 0);
      schemaLinkWriteIndex = 0;
    }

    this.currentEntryRepetitionLevel = child.currentEntryRepetitionLevel();
    this.currentEntryDefinitionLevel = child.currentEntryDefinitionLevel();

    boolean childLinkIsDefined = currentEntryDefinitionLevel >= child.nodeDefinitionLevel();
    boolean requiresNewList = currentEntryRepetitionLevel < repetitionLevelAtThisNode;

    if (childLinkIsDefined && requiresNewList) {
      if (numberOfItemsInList > 0) {
        schemaLinks[childColumnIndex][listHeaderIndex] = numberOfItemsInList;
        numberOfItemsInList = 0;
      }

      listHeaderIndex = schemaLinkWriteIndex++;
    }

    if (childLinkIsDefined) {
      schemaLinks[childColumnIndex][schemaLinkWriteIndex++] = child.currentLinkSiteIndex();
      this.numberOfItemsInList++;
    } else {
      schemaLinks[childColumnIndex][schemaLinkWriteIndex++] = null;
    }

    this.currentLinkSiteIndex = listHeaderIndex;

    // If we require a link from the parent:
    if (child.currentEntryRepetitionLevel() <= parentRepetitionLevel) {
      // If this node reports schema, then continue upstream:
      if (isSchemaReportingNode) {
        parent.linkSchema(this);
      }
    }

    this.schemaLinkWriteIndexForColumn[childColumnIndex] = schemaLinkWriteIndex;
  }

  @Override
  public void finishRow(IngestNode child) {
    if (numberOfItemsInList > 0) {
      // Todo: make this not fixed.
      int REPLACE_THIS_WITH_ACTUAL_CHILD_NUMBER = 0;
      this.schemaLinks[REPLACE_THIS_WITH_ACTUAL_CHILD_NUMBER][listHeaderIndex] = numberOfItemsInList;
    }

    // If this node reports schema, then continue upstream:
    if (child.isSchemaReportingNode()) {
      parent.finishRow(this);
    }
  }

}
