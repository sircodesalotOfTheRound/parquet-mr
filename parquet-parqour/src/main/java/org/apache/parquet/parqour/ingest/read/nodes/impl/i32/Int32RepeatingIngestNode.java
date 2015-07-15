package org.apache.parquet.parqour.ingest.read.nodes.impl.i32;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.parqour.ingest.cursor.iface.AdvanceableCursor;
import org.apache.parquet.parqour.ingest.cursor.iterable.i32.Int32IterableCursor;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.Int32FastForwardReader;
import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.read.nodes.categories.AggregatingIngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.categories.PrimitiveIngestNodeBase;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.schema.Type;

/**
 * Created by sircodesalot on 6/11/15.
 */
public final class Int32RepeatingIngestNode extends PrimitiveIngestNodeBase<Int32FastForwardReader> {
  private int currentValue = 0;

  private Integer[] ingestBuffer;
  private final Int32IterableCursor cursor;

  public Int32RepeatingIngestNode(SchemaInfo schemaInfo,
                                  AggregatingIngestNode parent,
                                  Type schemaNode,
                                  ColumnDescriptor descriptor,
                                  DiskInterfaceManager diskInterfaceManager,
                                  int childIndex) {

    super(schemaInfo, parent, schemaNode, descriptor, diskInterfaceManager, childIndex);

    this.ingestBufferLength = 100;
    this.ingestBuffer = new Integer[ingestBufferLength];
    this.cursor = new Int32IterableCursor(name, columnIndex, ingestBuffer);
  }

  @Override
  protected void updateValuesReaderValue() {
    this.currentValue = valuesReader.readi32();
  }

  @Override
  protected AdvanceableCursor onLinkToParent(AggregatingIngestNode parentNode) {
    return cursor;
  }

  // Heavily inlined for performance.
  @Override
  public void read(int rowNumber) {
    if (currentRowNumber > rowNumber) return;

    int writeIndex = 0;
    int listHeaderIndex = -1;
    int numberOfItemsInList = 0;

    do {
      if (currentRowNumber < rowNumber) {
        this.fastForwardToRow(rowNumber);
      }

      boolean isDefined = currentEntryDefinitionLevel >= definitionLevelAtThisNode;
      boolean requiresNewList = currentEntryRepetitionLevel < repetitionLevelAtThisNode;

      // Manage list creation:
      if (isDefined) {
        if (requiresNewList) {
          if (numberOfItemsInList > 0) {
            ingestBuffer[listHeaderIndex] = numberOfItemsInList;
            numberOfItemsInList = 0;
          }

          listHeaderIndex = writeIndex++;
        }

        // If the current item is defined, then set the link site to the head of the list.
        this.currentLinkSiteIndex = listHeaderIndex;
      } else {
        // If the current item is not defined, then set the link site to the value itself.
        this.currentLinkSiteIndex = writeIndex;
      }

      if (writeIndex >= ingestBufferLength) {
        this.expandIngestBuffer();
      }

      if (isDefined) {
        ingestBuffer[writeIndex++] = currentValue;
        numberOfItemsInList++;
      } else {
        ingestBuffer[writeIndex++] = null;
      }

      if (requiresNewList) {
        parent.linkSchema(this);
      }

      if (currentEntryOnPage < totalItemsOnThisPage) {
        this.currentEntryOnPage++;
        this.currentEntryRepetitionLevel = repetitionLevelReader.nextRelationshipLevel();
        this.currentEntryDefinitionLevel = definitionLevelReader.nextRelationshipLevel();

        // If we're defined at this node, update the value:
        if (currentEntryDefinitionLevel >= definitionLevelAtThisNode) {
          this.currentValue = valuesReader.readi32();
        }
      } else if (currentRowNumber < totalRowCount - 1) {
        super.moveToNextPage();

      } else {
        this.currentEntryDefinitionLevel = 0;
        this.currentEntryRepetitionLevel = 0;
        this.currentValue = -1;
      }
    } while (currentEntryRepetitionLevel > 0);

    if (numberOfItemsInList > 0) {
      ingestBuffer[listHeaderIndex] = numberOfItemsInList;
    }

    parent.finishRow(this);
    currentRowNumber++;
  }

  @Override
  public final void expandIngestBuffer() {
    int newIngestBufferLength = super.ingestBufferLength * 2;
    Integer[] newIngestBuffer = new Integer[newIngestBufferLength];
    System.arraycopy(this.ingestBuffer, 0, newIngestBuffer, 0, ingestBufferLength);

    this.ingestBuffer = newIngestBuffer;
    this.ingestBufferLength = newIngestBufferLength;
    this.cursor.setArray(newIngestBuffer);
  }
}
