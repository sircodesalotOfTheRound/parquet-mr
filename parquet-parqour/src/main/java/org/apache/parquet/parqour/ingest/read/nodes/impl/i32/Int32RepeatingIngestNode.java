package org.apache.parquet.parqour.ingest.read.nodes.impl.i32;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.parqour.ingest.cursor.iface.AdvanceableCursor;
import org.apache.parquet.parqour.ingest.cursor.iterable.i32.Int32IterableCursor;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.Int32FastForwardReader;
import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.read.nodes.categories.AggregatingIngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.categories.ColumnIngestNodeBase;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.schema.Type;

/**
 * Created by sircodesalot on 6/11/15.
 */
public final class Int32RepeatingIngestNode extends ColumnIngestNodeBase<Int32FastForwardReader> {
  private int currentValue = 0;

  // TODO: Write expansion code.
  private Integer[] rowVector = new Integer[100000];

  private final Int32IterableCursor cursor = new Int32IterableCursor(this.name, rowVector);

  public Int32RepeatingIngestNode(SchemaInfo schemaInfo,
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
  protected AdvanceableCursor onLinkToParent(AggregatingIngestNode parentNode) {
    return cursor;
  }

  // Heavily inlined for performance.
  @Override
  public void read(int rowNumber) {
    if (currentRowNumber > rowNumber) return;

    int writeIndex = -1;
    int listHeaderIndex = -1;
    int numberOfItemsInList = 0;
    this.relationshipLinkWriteIndex = -1;

    // Repeat until we reach a node with repetitionLevel-0 (new row) or EOF.
    do {
      // If the current row is behind the row we need to read:
      if (currentRowNumber < rowNumber) {
        this.fastForwardToRow(rowNumber);
      }

      boolean isDefined = currentEntryDefinitionLevel >= definitionLevelAtThisNode;
      boolean requiresNewList = currentEntryRepetitionLevel < repetitionLevelAtThisNode;

      // Manage list creation:
      if (isDefined && requiresNewList) {
        if (numberOfItemsInList > 0) {
          rowVector[listHeaderIndex] = numberOfItemsInList;
          numberOfItemsInList = 0;
        }

        listHeaderIndex = ++writeIndex;
      }

      if (isDefined) {
        rowVector[++writeIndex] = currentValue;
        numberOfItemsInList++;
      } else {
        rowVector[++writeIndex] = null;
      }

      if (requiresNewList) {
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
        this.currentEntryDefinitionLevel = 0;
        this.currentEntryRepetitionLevel = 0;
        this.currentValue = -1;

      }

    } while (currentEntryRepetitionLevel > 0);

    // Close out the list:
    if (numberOfItemsInList > 0) {
      rowVector[listHeaderIndex] = numberOfItemsInList;
    }

    // If this node reports schema:
    if (isSchemaReportingNode) {
      parent.finishRow();
    }

    // Increment the row number:
    currentRowNumber++;
  }

}
