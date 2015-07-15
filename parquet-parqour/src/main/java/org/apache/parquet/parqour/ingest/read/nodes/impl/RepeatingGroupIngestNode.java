package org.apache.parquet.parqour.ingest.read.nodes.impl;

import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.read.nodes.categories.AggregatingIngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.categories.IngestNode;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.schema.GroupType;

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
    if (currentRowNumber != child.currentRowNumber()) {
      this.relationshipLinkWriteIndex = -1;
      this.currentRowNumber = child.currentRowNumber();

      this.listHeaderIndex = -1;
      this.numberOfItemsInList = 0;
    }

    this.currentEntryRepetitionLevel = child.currentEntryRepetitionLevel();
    this.currentEntryDefinitionLevel = child.currentEntryDefinitionLevel();

    boolean childLinkIsDefined = currentEntryDefinitionLevel >= child.nodeDefinitionLevel();
    boolean requiresNewList = currentEntryRepetitionLevel < repetitionLevelAtThisNode;

    int childColumnIndex = child.columnIndex();
    if (childLinkIsDefined && requiresNewList) {
      if (numberOfItemsInList > 0) {
        schemaLinks[childColumnIndex][listHeaderIndex] = numberOfItemsInList;
        numberOfItemsInList = 0;
      }

      listHeaderIndex = ++relationshipLinkWriteIndex;
    }

    if (childLinkIsDefined) {
      schemaLinks[childColumnIndex][++relationshipLinkWriteIndex] = child.currentLinkSiteIndex();
      this.numberOfItemsInList++;
    } else {
      schemaLinks[childColumnIndex][++relationshipLinkWriteIndex] = null;
    }

    this.currentLinkSiteIndex = listHeaderIndex;

    // If we require a link from the parent:
    if (child.currentEntryRepetitionLevel() <= parentRepetitionLevel) {
      // If this node reports schema, then continue upstream:
      if (isSchemaReportingNode) {
        parent.linkSchema(this);
      }
    }
  }

  @Override
  public void finishRow() {
    if (numberOfItemsInList > 0) {
      // Todo: make this not fixed.
      Integer[] schemaLinks = this.cursor().getlinksForChild(0);
      schemaLinks[listHeaderIndex] = numberOfItemsInList;
    }

    // If this node reports schema, then continue upstream:
    if (isSchemaReportingNode) {
      parent.finishRow();
    }
  }

}
