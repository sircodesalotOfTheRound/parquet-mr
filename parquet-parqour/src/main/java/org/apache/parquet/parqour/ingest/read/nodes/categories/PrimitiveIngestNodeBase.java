package org.apache.parquet.parqour.ingest.read.nodes.categories;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.ingest.disk.manager.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.disk.pages.Page;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.FastForwardReader;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.RelationshipLevelFastForwardReader;
import org.apache.parquet.parqour.ingest.paging.DataPageDecorator;
import org.apache.parquet.parqour.ingest.schema.QueryInfo;
import org.apache.parquet.schema.Type;

import java.util.Iterator;

/**
 * Created by sircodesalot on 6/2/15.
 */
public abstract class PrimitiveIngestNodeBase<TFFReaderType extends FastForwardReader> extends IngestNode {
  protected static final int NEW_RECORD = 0;

  protected final ColumnDescriptor columnDescriptor;
  protected final DiskInterfaceManager diskInterfaceManager;

  protected Iterator<Page> pager;
  protected Page page;

  protected RelationshipLevelFastForwardReader definitionLevelReader;
  protected RelationshipLevelFastForwardReader repetitionLevelReader;
  protected TFFReaderType valuesReader;

  protected long currentEntryOnPage;
  protected long totalItemsOnThisPage;

  public PrimitiveIngestNodeBase(QueryInfo queryInfo, AggregatingIngestNode parent,
                                 Type schemaNode, ColumnDescriptor columnDescriptor,
                                 DiskInterfaceManager diskInterfaceManager, int childIndex) {

    super(queryInfo, parent, ColumnPath.get(columnDescriptor.getPath()).toDotString(),
      schemaNode, IngestNodeCategory.DATA_INGEST, childIndex);

    this.validateNode();

    this.diskInterfaceManager = diskInterfaceManager;
    this.columnDescriptor = columnDescriptor;
    this.pager = getPager(diskInterfaceManager);
    this.page = readFirstPage(pager);
  }

  // First page may be null in case of no items.
  private Iterator<Page> getPager(DiskInterfaceManager diskInterfaceManager) {
    if (diskInterfaceManager.containsPagerFor(path)) {
      return diskInterfaceManager.pagerFor(path).iterator();
    } else {
      return null;
    }
  }

  private Page readFirstPage(Iterator<Page> pager) {
    if (pager != null && pager.hasNext()) {
      Page page = pager.next();
      this.onPageRead(page);
      return page;
    } else {
      return null;
    }
  }

  public void validateNode() {
    if (!this.hasParent) {
      throw new DataIngestException("Record-reading ingest nodes nodes must have a parent.");
    }
  }

  protected void fastForwardToRow(int rowNumber) {
    if (canPerformTrueFastForwards) {
      this.performFastForwardTo(rowNumber);
    } else {
      this.performSlowForwardTo(rowNumber);
    }
  }

  private void performFastForwardTo(int rowNumber) {
    if (currentRowNumber != rowNumber) {
      this.moveToPageContainingRowNumber(rowNumber);

      this.definitionLevelReader.fastForwardTo(rowNumber);
      this.repetitionLevelReader.fastForwardTo(rowNumber);
      this.valuesReader.fastForwardTo(rowNumber);

      this.onPreReadFirstRecordOnPage();

      this.currentEntryOnPage = (rowNumber - page.firstEntry());
    }
  }

  protected final void moveToNextPage() {
    this.page = pager.next();
    this.onPageRead(page);
  }

  private void moveToPageContainingRowNumber(int entryNumber) {
    while (!this.page.containsEntry(entryNumber)) {
      this.page = pager.next();
    }

    this.totalItemsOnThisPage = page.totalEntries();
    this.onPageRead(page);
  }

  private void performSlowForwardTo(long rowNumber) {
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
      if (currentEntryRepetitionLevel == NEW_RECORD) {
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

  private void onPageRead(Page page) {
    this.definitionLevelReader = page.definitionLevelReader();
    this.repetitionLevelReader = page.repetitionLevelReader();
    this.valuesReader = page.contentReader();

    this.currentEntryOnPage = 0;
    this.totalItemsOnThisPage = page.totalEntries();
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

    currentEntryOnPage++;
  }

  public abstract void read(int rowNumber);
}
