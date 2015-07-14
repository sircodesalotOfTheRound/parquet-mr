package org.apache.parquet.parqour.ingest.read.nodes.impl.i32;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.parqour.ingest.cursor.Int32Cursor;
import org.apache.parquet.parqour.ingest.cursor.iface.AdvanceableCursor;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.Int32FastForwardReader;
import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.read.nodes.categories.AggregatingIngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.categories.ColumnIngestNodeBase;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.schema.Type;

/**
 * Created by sircodesalot on 6/11/15.
 */
public final class Int32NoRepeatIngestNode extends ColumnIngestNodeBase<Int32FastForwardReader> {
  private int currentValue = 0;

  private int ingestBufferLength;
  private Integer[] ingestBuffer;
  private final Int32Cursor cursor;

  public Int32NoRepeatIngestNode(SchemaInfo schemaInfo,
                                 AggregatingIngestNode parent,
                                 Type schemaNode,
                                 ColumnDescriptor descriptor,
                                 DiskInterfaceManager diskInterfaceManager,
                                 int childIndex) {

    super(schemaInfo, parent, schemaNode, descriptor, diskInterfaceManager, childIndex);

    this.ingestBufferLength = 100;
    this.ingestBuffer = new Integer[ingestBufferLength];
    this.cursor = new Int32Cursor(name, ingestBuffer);
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

    this.currentLinkSiteIndex = 0;

    // Repeat until we reach a node with repetitionLevel-0 (new row) or EOF.
    do {
      // If the current row is behind the row we need to read:
      if (currentRowNumber < rowNumber) {
        this.fastForwardToRow(rowNumber);
      }

      // If this node is defined:
      if (currentEntryDefinitionLevel >= definitionLevelAtThisNode) {
        ingestBuffer[currentLinkSiteIndex++] = currentValue;
      } else {
        ingestBuffer[currentLinkSiteIndex++] = null;
      }

      // If this node requires linking to it's parent:
      if (currentEntryRepetitionLevel <= parentDefinitionLevel) {
        parent.linkSchema(this);
      }

      // If:
      // (1) there are still items on this page:
      // (2) there are no more items on this page, but there are rows left to read:
      // (3) No more rows left to read:
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
        this.currentEntryDefinitionLevel = 0;this.currentEntryRepetitionLevel = 0;
        this.currentValue = -1;

      }

    } while (currentEntryRepetitionLevel > 0);

    // If this node reports schema:
    if (isSchemaReportingNode) {
      parent.finishRow();
    }

    // Increment the row number:
    currentRowNumber++;
  }

  @Override
  protected final void expandIngestBuffer() {
    int newIngestBufferLength = this.ingestBufferLength * 2;
    Integer[] newIngestBuffer = new Integer[newIngestBufferLength];
    System.arraycopy(this.ingestBuffer, 0, newIngestBuffer, 0, ingestBufferLength);

    this.ingestBuffer = newIngestBuffer;
    this.ingestBufferLength = newIngestBufferLength;
    this.cursor.setArray(newIngestBuffer);
  }
}
