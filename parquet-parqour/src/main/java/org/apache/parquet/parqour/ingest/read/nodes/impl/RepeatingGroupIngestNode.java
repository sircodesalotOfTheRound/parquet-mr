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

  private int relationshipWriteIndexForChild;
  private int listHeaderIndex;
  private int numberOfItemsInList;

  public RepeatingGroupIngestNode(SchemaInfo schemaInfo, AggregatingIngestNode aggregatingIngestNode, String childPath, GroupType child, DiskInterfaceManager diskInterfaceManager, int childColumnIndex) {
    super(schemaInfo, aggregatingIngestNode, childPath, child, diskInterfaceManager, childColumnIndex);

    this.relationshipWriteIndexForChild = -1;
    this.listHeaderIndex = -1;
    this.numberOfItemsInList = 0;
  }

  @Override
  public final void linkSchema(IngestNode child) {
    if (currentRowNumber != child.currentRowNumber()) {
      relationshipLinkWriteIndex = -1;
      currentRowNumber = child.currentRowNumber();

      this.listHeaderIndex = -1;
      this.numberOfItemsInList = 0;
    }

    // If the parent is defined:
    /*if (child.currentEntryDefinitionLevel() >= definitionLevelAtThisNode) {
      schemaLinksFromParentToChild[++relationshipLinkWriteIndex] = child.currentLinkSiteIndex();
    } else {
      schemaLinksFromParentToChild[++relationshipLinkWriteIndex] = null;
    }*/

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
    // If this node reports schema, then continue upstream:
    if (isSchemaReportingNode) {
      parent.finishRow();
    }
  }

  @Override
  public int determineWriteIndexForRelationshipList(int definitionLevel, int repetitionLevel, int columnIndex, boolean childIsDefined) {
    Integer[] schemaLinksForChild = this.collectAggregate().getlinksForChild(columnIndex);

    boolean isDefined = (definitionLevel >= definitionLevelAtThisNode);
    boolean requiresSchemaLinkFromParent = (repetitionLevel <= parentRepetitionLevel);

    if (isDefined) {
      if (requiresSchemaLinkFromParent) {
        if (numberOfItemsInList > 0) {
          schemaLinksForChild[listHeaderIndex] = numberOfItemsInList;
          numberOfItemsInList = 0;
        }

        listHeaderIndex = ++relationshipWriteIndexForChild;
      }
    } else {
      if (numberOfItemsInList > 0) {
        schemaLinksForChild[listHeaderIndex] = numberOfItemsInList;
        numberOfItemsInList = 0;
      }

      listHeaderIndex = ++relationshipWriteIndexForChild;
      schemaLinksForChild[listHeaderIndex] = null;
    }

    return ++relationshipWriteIndexForChild;
  }

  @Override
  public void endSchemaRepetitionList() {

  }
}
