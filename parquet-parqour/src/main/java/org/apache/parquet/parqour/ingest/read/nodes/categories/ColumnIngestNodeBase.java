package org.apache.parquet.parqour.ingest.read.nodes.categories;

import org.apache.parquet.parqour.exceptions.ReadNodeException;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.FastForwardReader;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.RelationshipLevelFastForwardReader;
import org.apache.parquet.parqour.ingest.paging.DataPageDecorator;
import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.read.nodes.impl.Int32IngestNode;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.Type;

/**
 * Created by sircodesalot on 6/2/15.
 */
public abstract class ColumnIngestNodeBase<TFFReaderType extends FastForwardReader> extends IngestNode {
  protected final ColumnDescriptor columnDescriptor;
  protected final DiskInterfaceManager diskInterfaceManager;
  protected DataPageDecorator dataPage;

  protected int currentEntryDefinitionLevel;
  protected int currentEntryRepetitionLevel;

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
    if (this.parent() == null) throw new ReadNodeException("Record-reading ingest nodes nodes must have a parent.");
  }

  protected void performFastForward(int rowNumber) {
    moveToPageContainingRowNumber(rowNumber);

    definitionLevelReader.fastForwardTo(rowNumber);
    repetitionLevelReader.fastForwardTo(rowNumber);
    valuesReader.fastForwardTo(rowNumber);

    this.onPreReadFirstRecordOnPage();
  }

  public void moveToNextPage() {
    this.dataPage = diskInterfaceManager.getNextPageForColumn(dataPage);
    this.totalItemsOnThisPage = dataPage.totalItems();

    this.onPageRead(dataPage);
  }

  public void moveToPageContainingRowNumber(int rowNumber) {
    while (!this.dataPage.containsRow(rowNumber)) {
      this.dataPage = diskInterfaceManager.getNextPageForColumn(dataPage);
    }

    this.totalItemsOnThisPage = dataPage.totalItems();
    this.onPageRead(dataPage);
  }

  public void onPageRead(DataPageDecorator page) {
    this.definitionLevelReader = page.definitionLevelReader();
    this.repetitionLevelReader = page.repetitionLevelReader();
    this.valuesReader = page.valuesReader();

    this.currentEntryOnPage = 0;
    this.onPreReadFirstRecordOnPage();
  }

  public abstract void onPreReadFirstRecordOnPage();

  @Deprecated
  public void createRelationshipLink(int resultIndex) {
    //parent.onSchemaChanged(thisChildColumnIndex, currentEntryDefinitionLevel, currentEntryRepetitionLevel, resultIndex);
  }

  protected void reportResults(int rowNumber) {
    parent.setResultsReported(rowNumber, this.thisChildColumnIndex);
  }

  public static ColumnIngestNodeBase determineReadNodeType(SchemaInfo schemaInfo, AggregatingIngestNode parent, ColumnDescriptor descriptor,
                                                           Type schemaNode, DiskInterfaceManager diskInterfaceManager, int childIndex) {
    return new Int32IngestNode(schemaInfo, parent, schemaNode, descriptor, diskInterfaceManager, childIndex);
  }

  public abstract void read(int rowNumber);
}
