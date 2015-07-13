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
    this.relationshipLinks = relationships;
    return cursor;
  }

  // Heavily inlined for performance.
  @Override
  public void read(int rowNumber) {
    // (0) Initialize for writing new row.
    boolean lastItemWasNull = false;
    int writeIndex = -1;

    this.relationshipLinkWriteIndex = -1;

    // (1) Repeat until we reach a repetition-level of 'zero' (beginning of a new record).
    do {
      // (2) If the current row number is not the same as the row number to be read, perform a fast forward.
      if (currentRowNumber < rowNumber) {
        this.forwardToRowNumber(rowNumber);
      }

      boolean isDefined = currentEntryDefinitionLevel >= definitionLevelAtThisNode;

      // If the value is defined, write the value. Else write null.
      if (isDefined) {
        rowVector[++writeIndex] = currentValue;
      } else {
        rowVector[++writeIndex] = null;
      }

      // If the repetition level is lower than the parent node's repetition level, then link.
      if (currentEntryRepetitionLevel <= parentRepetitionLevel) {
        // TODO: Should this be if parent is defined??
        //if (isDefined) {
          // Single entry represents the beginning of a new range (and ending of the previous).
          relationshipLinks[++relationshipLinkWriteIndex] = writeIndex;

        //} else {
        //}

        if (isSchemaReportingNode) {
          parent.setSchemaLink(rowNumber, currentEntryRepetitionLevel, currentEntryDefinitionLevel, writeIndex);
        }
      }

      // (5) Pre-read the content for the next row (if there is data to read).
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

    relationshipLinks[++relationshipLinkWriteIndex] = ++writeIndex;

    if (isSchemaReportingNode) {
      parent.finishRow(writeIndex);
    }

    currentRowNumber++;
  }

}
