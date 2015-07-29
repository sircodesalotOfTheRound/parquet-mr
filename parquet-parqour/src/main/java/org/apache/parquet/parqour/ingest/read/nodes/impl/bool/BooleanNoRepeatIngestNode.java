package org.apache.parquet.parqour.ingest.read.nodes.impl.bool;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.parqour.ingest.cursor.iface.AdvanceableCursor;
import org.apache.parquet.parqour.ingest.cursor.implementations.noniterable.bool.BooleanCursor;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.BooleanFastForwardReader;
import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.read.nodes.categories.AggregatingIngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.categories.PrimitiveIngestNodeBase;
import org.apache.parquet.parqour.ingest.schema.QueryInfo;
import org.apache.parquet.schema.Type;

import java.util.Arrays;

/**
 * Created by sircodesalot on 6/11/15.
 */
public final class BooleanNoRepeatIngestNode extends PrimitiveIngestNodeBase<BooleanFastForwardReader> {
  private Boolean currentValue;
  private Boolean[] ingestBuffer;
  private final BooleanCursor cursor;

  public BooleanNoRepeatIngestNode(QueryInfo queryInfo,
                                   AggregatingIngestNode parent,
                                   Type schemaNode,
                                   ColumnDescriptor descriptor,
                                   DiskInterfaceManager diskInterfaceManager,
                                   int childIndex) {

    super(queryInfo, parent, schemaNode, descriptor, diskInterfaceManager, childIndex);

    this.ingestBufferLength = 100;
    this.ingestBuffer = new Boolean[ingestBufferLength];
    this.cursor = new BooleanCursor(name, columnIndex, ingestBuffer);
  }

  @Override
  protected void updateValuesReaderValue() {
    this.currentValue = valuesReader.readtf();
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
          this.currentValue = valuesReader.readtf();
        }
      } else if (currentRowNumber < totalRowCount - 1) {
        super.moveToNextPage();
      } else {
        this.currentEntryDefinitionLevel = 0;
        this.currentEntryRepetitionLevel = 0;
        this.currentValue = false;
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
