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

import java.util.Arrays;

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

    do {
      if (currentRowNumber < rowNumber) {
        this.fastForwardToRow(rowNumber);
      }

      boolean isDefined = currentEntryDefinitionLevel == definitionLevelAtThisNode;
      boolean requiresNewList = currentEntryRepetitionLevel < repetitionLevelAtThisNode;

      if (requiresNewList) {
        // Create a new list-header at 'currentLinkSiteIndex'.
        this.currentLinkSiteIndex = writeIndex++;
        parent.linkSchema(this);

        // Set the list-header to 'zero'.
        ingestBuffer[currentLinkSiteIndex] = 0;
      }

      if (writeIndex >= ingestBufferLength) {
        this.expandIngestBuffer();
      }

      // If the value is defined, then write it and increment the list-header.
      if (isDefined) {
        ingestBuffer[writeIndex++] = currentValue;
        ingestBuffer[currentLinkSiteIndex]++;
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
    } while (currentEntryRepetitionLevel != NEW_RECORD);

    currentRowNumber++;
  }

  @Override
  public final void expandIngestBuffer() {
    this.ingestBuffer = Arrays.copyOf(ingestBuffer, ingestBufferLength * 2);
    this.ingestBufferLength = ingestBuffer.length;
    this.cursor.setArray(ingestBuffer);
  }
}
