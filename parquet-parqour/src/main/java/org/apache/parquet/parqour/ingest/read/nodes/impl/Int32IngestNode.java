package org.apache.parquet.parqour.ingest.read.nodes.impl;

import org.apache.parquet.parqour.ingest.cursor.iface.AdvanceableCursor;
import org.apache.parquet.parqour.ingest.cursor.Int32Cursor;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.Int32FastForwardReader;
import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.read.nodes.categories.AggregatingIngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.categories.ColumnIngestNodeBase;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.Type;

/**
 * Created by sircodesalot on 6/11/15.
 */
public final class Int32IngestNode extends ColumnIngestNodeBase<Int32FastForwardReader> {
  private int currentValue = 0;

  // TODO: Write expansion code.
  private Integer[] rowVector = new Integer[100000];

  private final Int32Cursor cursor = new Int32Cursor(this.name, rowVector);

  public Int32IngestNode(SchemaInfo schemaInfo,
                         AggregatingIngestNode parent,
                         Type schemaNode,
                         ColumnDescriptor descriptor,
                         DiskInterfaceManager diskInterfaceManager,
                         int childIndex) {

    super(schemaInfo, parent, schemaNode, descriptor, diskInterfaceManager, childIndex);

  }

  @Override
  protected void updateValuesReaderValue() {
    this.currentValue = valuesReader.readi32();
  }

  @Override
  protected AdvanceableCursor onLinkToParent(AggregatingIngestNode parentNode, Integer[] relationships) {
    this.schemaLinksFromParentToChild = relationships;
    return cursor;
  }

  // Heavily inlined for performance.
  @Override
  public void read(int rowNumber) {
    // (0) Initialize for writing new row.
    boolean lastItemWasNull = false;
    int writeIndex = -1;

    this.relationshipLinkWriteIndex = -1;

    // Repeat until we reach the
    do {
      // (2) If the current row number is not the same as the row number to be read, perform a fast forward.
      if (currentRowNumber < rowNumber) {
        this.forwardToRowNumber(rowNumber);
      }

      // If this node is defined:
      if (currentEntryDefinitionLevel >= definitionLevelAtThisNode) {
        rowVector[++writeIndex] = currentValue;
      } else {
        rowVector[++writeIndex] = null;
      }

      // If this node requires linking to it's parent:
      if (currentEntryRepetitionLevel >= parentDefinitionLevel) {
        // If the parent is defined:
        if (currentEntryDefinitionLevel >= parentDefinitionLevel) {
          schemaLinksFromParentToChild[++relationshipLinkWriteIndex] = writeIndex;
        } else {
          schemaLinksFromParentToChild[++relationshipLinkWriteIndex] = null;
        }

        // Continue upstream if this node is schema-defining.
        if (isSchemaReportingNode) {
          parent.setSchemaLink(rowNumber, currentEntryRepetitionLevel, currentEntryDefinitionLevel, writeIndex);
        }
      }

      if (currentEntryOnPage < totalItemsOnThisPage) {
        this.currentEntryOnPage += 1;

        // (5a-1) If there is still content on this page, then update the relationship levels.
        this.currentEntryRepetitionLevel = repetitionLevelReader.nextRelationshipLevel();
        this.currentEntryDefinitionLevel = definitionLevelReader.nextRelationshipLevel();

        // (5a-2) If we are defined at this node, then read the value.
        if (currentEntryDefinitionLevel >= definitionLevelAtThisNode) {
          this.currentValue = valuesReader.readi32();
        }

      } else if (currentRowNumber < totalRowCount - 1) {
        // (5b) Read the next page if there is more data to read.
        super.moveToNextPage();

      } else {
        // (5c) No more values left to read, set all values to invalid numbers.
        this.currentEntryDefinitionLevel = 0;
        this.currentEntryRepetitionLevel = 0;
        this.currentValue = -1;

      }

    } while (currentEntryRepetitionLevel > 0);

    schemaLinksFromParentToChild[++relationshipLinkWriteIndex] = ++writeIndex;

    if (isSchemaReportingNode) {
      parent.finishRow(writeIndex);
    }

    currentRowNumber++;
  }

}
