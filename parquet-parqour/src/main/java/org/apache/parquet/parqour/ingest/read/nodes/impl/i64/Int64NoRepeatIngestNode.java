package org.apache.parquet.parqour.ingest.read.nodes.impl.i64;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.parqour.cursor.iface.AdvanceableCursor;
import org.apache.parquet.parqour.cursor.implementations.noniterable.i64.Int64Cursor;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.Int64FastForwardReader;
import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.read.nodes.categories.AggregatingIngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.categories.PrimitiveIngestNodeBase;
import org.apache.parquet.parqour.ingest.schema.QueryInfo;
import org.apache.parquet.schema.Type;

import java.util.Arrays;

/**
 * Created by sircodesalot on 6/11/15.
 */
public final class Int64NoRepeatIngestNode extends PrimitiveIngestNodeBase<Int64FastForwardReader> {
  private long currentValue;
  private Long[] ingestBuffer;
  private final Int64Cursor cursor;

  public Int64NoRepeatIngestNode(QueryInfo queryInfo,
                                 AggregatingIngestNode parent,
                                 Type schemaNode,
                                 ColumnDescriptor descriptor,
                                 DiskInterfaceManager diskInterfaceManager,
                                 int childIndex) {

    super(queryInfo, parent, schemaNode, descriptor, diskInterfaceManager, childIndex);

    this.ingestBufferLength = 100;
    this.ingestBuffer = new Long[ingestBufferLength];
    this.cursor = new Int64Cursor(name, columnIndex, ingestBuffer);
  }

  @Override
  protected void updateValuesReaderValue() {
    this.currentValue = valuesReader.readi64();
  }


  @Override
  protected AdvanceableCursor onLinkToParent(AggregatingIngestNode parentNode) {
    return cursor;
  }

  @Override
  public void read(int rowNumber) {
    if (currentRowNumber > rowNumber) return;

    this.currentLinkSiteIndex = -1;

    do {
      if (currentRowNumber < rowNumber) {
        this.fastForwardToRow(rowNumber);
      }

      if (currentLinkSiteIndex >= ingestBufferLength) {
        this.expandIngestBuffer();
      }

      if (currentEntryDefinitionLevel == definitionLevelAtThisNode) {
        ingestBuffer[++currentLinkSiteIndex] = currentValue;
      } else {
        ingestBuffer[++currentLinkSiteIndex] = null;
      }

      parent.linkSchema(this);

      if (currentEntryOnPage < totalItemsOnThisPage) {
        this.currentEntryOnPage++;
        this.currentEntryRepetitionLevel = repetitionLevelReader.nextRelationshipLevel();
        this.currentEntryDefinitionLevel = definitionLevelReader.nextRelationshipLevel();

        // If we're defined at this node, update the value:
        if (currentEntryDefinitionLevel >= definitionLevelAtThisNode) {
          this.currentValue = valuesReader.readi64();
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
  protected final void expandIngestBuffer() {
    this.ingestBuffer = Arrays.copyOf(ingestBuffer, ingestBufferLength * 2);
    this.ingestBufferLength = ingestBuffer.length;

    this.cursor.setArray(ingestBuffer);
  }
}
