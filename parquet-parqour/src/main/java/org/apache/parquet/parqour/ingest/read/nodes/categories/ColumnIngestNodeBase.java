package org.apache.parquet.parqour.ingest.read.nodes.categories;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.FastForwardReader;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.RelationshipLevelFastForwardReader;
import org.apache.parquet.parqour.ingest.paging.DataPageDecorator;
import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.schema.Type;

/**
 * Created by sircodesalot on 6/2/15.
 */
public abstract class ColumnIngestNodeBase<TFFReaderType extends FastForwardReader> extends IngestNode {
  protected final ColumnDescriptor columnDescriptor;
  protected final DiskInterfaceManager diskInterfaceManager;
  protected DataPageDecorator dataPage;


  protected RelationshipLevelFastForwardReader definitionLevelReader;
  protected RelationshipLevelFastForwardReader repetitionLevelReader;
  protected TFFReaderType valuesReader;

  protected int currentEntryOnPage;
  protected long totalItemsOnThisPage;

  public ColumnIngestNodeBase(SchemaInfo schemaInfo, AggregatingIngestNode parent,
                              Type schemaNode, ColumnDescriptor columnDescriptor,
                              DiskInterfaceManager diskInterfaceManager, int childIndex) {
    super(schemaInfo, parent, ColumnPath.get(columnDescriptor.getPath()).toDotString(),
      schemaNode, IngestNodeCategory.DATA_INGEST, childIndex);

    this.validateNode();

    this.diskInterfaceManager = diskInterfaceManager;
    this.columnDescriptor = columnDescriptor;
    this.dataPage = readFirstPage(diskInterfaceManager);

    this.totalItemsOnThisPage = dataPage.totalItems();
  }

  private DataPageDecorator readFirstPage(DiskInterfaceManager diskInterfaceManager) {
    DataPageDecorator page = diskInterfaceManager.getFirstPageForColumn(columnDescriptor);
    this.onPageRead(page);

    return page;
  }


  public void validateNode() {
    if (this.parent() == null) throw new DataIngestException("Record-reading ingest nodes nodes must have a parent.");
  }

  protected void fastForwardToRow(int rowNumber) {
    if (canPerformTrueFastForwards) {
      performFastForwardTo(rowNumber);
    } else {
      this.performSlowForwardTo(rowNumber);
    }
  }

  private void performFastForwardTo(int rowNumber) {
    if (currentRowNumber != rowNumber) {
      moveToPageContainingRowNumber(rowNumber);

      definitionLevelReader.fastForwardTo(rowNumber);
      repetitionLevelReader.fastForwardTo(rowNumber);
      valuesReader.fastForwardTo(rowNumber);

      this.onPreReadFirstRecordOnPage();
      currentEntryOnPage = rowNumber - dataPage.startingEntryNumber();
    }
  }

  protected final void moveToNextPage() {
    this.dataPage = diskInterfaceManager.getNextPageForColumn(dataPage);
    this.totalItemsOnThisPage = dataPage.totalItems();

    this.onPageRead(dataPage);
  }

  protected final void moveToPageContainingRowNumber(int rowNumber) {
    while (!this.dataPage.containsRow(rowNumber)) {
      this.dataPage = diskInterfaceManager.getNextPageForColumn(dataPage);
    }

    this.totalItemsOnThisPage = dataPage.totalItems();
    this.onPageRead(dataPage);
  }

  protected final void performSlowForwardTo(long rowNumber) {
    int valuesEntryNumber = 0;
    while (currentRowNumber != rowNumber) {
      // If there are no more items on this page, move to the next.
      // And reset the values entry number.
      if (currentEntryOnPage >= totalItemsOnThisPage) {
        this.moveToNextPage();
        valuesEntryNumber = 0;
      }

      // Read the RL/DL values for this entry.
      currentEntryRepetitionLevel = repetitionLevelReader.nextRelationshipLevel();
      currentEntryDefinitionLevel = definitionLevelReader.nextRelationshipLevel();

      // If the value is defined at this level, then increment the number of values we need to fast forward to.
      // Notice that we fast-forward the values entry only after we move to the right entry number.
      if (currentEntryDefinitionLevel == definitionLevelAtThisNode) {
        valuesEntryNumber++;
      }

      // Move the row number forward if we reach a repetition level of ZERO, since ZERO means 'new record'.
      if (currentEntryRepetitionLevel == 0) {
        currentRowNumber++;
      }

      // Move the current entry number forward.
      currentEntryOnPage++;
    }

    // Fast forward the values reader to this entry.
    valuesReader.fastForwardTo(valuesEntryNumber);

    // If defined for this record, update the value at this entry.
    if (currentEntryDefinitionLevel == definitionLevelAtThisNode) {
      this.updateValuesReaderValue();
    }
  }

  protected abstract void updateValuesReaderValue();

  public void onPageRead(DataPageDecorator page) {
    this.definitionLevelReader = page.definitionLevelReader();
    this.repetitionLevelReader = page.repetitionLevelReader();
    this.valuesReader = page.valuesReader();

    this.currentEntryOnPage = 0;
    this.onPreReadFirstRecordOnPage();
  }

  public final void onPreReadFirstRecordOnPage() {
    if (currentRowNumber < totalRowCount - 1) {
      this.currentEntryDefinitionLevel = definitionLevelReader.nextRelationshipLevel();
      this.currentEntryRepetitionLevel = repetitionLevelReader.nextRelationshipLevel();

      // Update the value if the definition level matches the value at this node.
      if (currentEntryDefinitionLevel >= definitionLevelAtThisNode) {
        this.updateValuesReaderValue();
      }
    }

    currentEntryOnPage += 1;
  }


  protected abstract void expandIngestBuffer();
  protected void reportResults(int rowNumber) {
    parent.setResultsReported(rowNumber, this.columnIndex);
  }

  public abstract void read(int rowNumber);
}
