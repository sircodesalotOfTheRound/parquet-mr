package org.apache.parquet.parqour.ingest.paging;

import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by sircodesalot on 6/9/15.
 */
public class DiskInterfaceManager {
  private final ParquetFileReader diskReader;
  private final Map<ColumnDescriptor, LinkedList<RowGroup>> pageStoresByColumn;

  public DiskInterfaceManager(SchemaInfo schemaInfo) {
    this.diskReader = generateDiskReader(schemaInfo);
    this.pageStoresByColumn = generatePageStoreSet(schemaInfo);
  }

  private ParquetFileReader generateDiskReader(SchemaInfo schemaInfo) {
    try {
      List<BlockMetaData> blocks = schemaInfo.blocks();
      List<ColumnDescriptor> columns = schemaInfo.columnDescriptors();

      return new ParquetFileReader(schemaInfo.configuration(), schemaInfo.path(), blocks, columns);
    } catch (IOException ex) {
      throw new DataIngestException("Failed to read because as there was an IO Exception.");
    }
  }

  private Map<ColumnDescriptor, LinkedList<RowGroup>> generatePageStoreSet(SchemaInfo schemaInfo) {
    try {
      HashMap<ColumnDescriptor, LinkedList<RowGroup>> pageStoreByColumnMap
        = new HashMap<ColumnDescriptor, LinkedList<RowGroup>>();

      RowGroup group = new RowGroup(diskReader.readNextRowGroup());
      for (ColumnDescriptor column : schemaInfo.columnDescriptors()) {
        LinkedList<RowGroup> pageList = new LinkedList<RowGroup>();
        pageList.add(group);

        pageStoreByColumnMap.put(column, pageList);
      }

      return pageStoreByColumnMap;
    } catch (IOException ex) {
      throw new DataIngestException("Unable to read next page");
    }
  }


  public DataPageDecorator getFirstPageForColumn(ColumnDescriptor columnDescriptor) {
    return fastForwardToPageContainingRow(columnDescriptor, 0, 0);
  }

  public DataPageDecorator getNextPageForColumn(DataPageDecorator previousPage) {
    ColumnDescriptor columnDescriptor = previousPage.columnDescriptor();
    return fastForwardToPageContainingRow(columnDescriptor, previousPage.finalRowNumber() + 1, previousPage.finalRowNumber() + 1);
  }

  private DataPageDecorator fastForwardToPageContainingRow(ColumnDescriptor column, int startingRowNumber, int rowNumber) {
    int onePastLastRowOnPage = startingRowNumber; // Initial value, we'll add the total page count shortly.
    while (true) {
      LinkedList<RowGroup> pageList = pageStoresByColumn.get(column);
      if (pageList.isEmpty()) {
        addRowGroupToAllColumns();
      }

      RowGroup rowGroup = pageList.removeFirst();
      PageReader pageReader = rowGroup.readPageForColumn(column);
      onePastLastRowOnPage += pageReader.getTotalValueCount();

      if (rowNumber < onePastLastRowOnPage) {
        DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
        return new DataPageDecorator(pageReader.readPage(), dictionaryPage, column, startingRowNumber);
      }
    }
  }

  private void addRowGroupToAllColumns() {
    try {
      RowGroup group = new RowGroup(this.diskReader.readNextRowGroup());
      for (ColumnDescriptor column : pageStoresByColumn.keySet()) {
        pageStoresByColumn.get(column).addLast(group);
      }
    } catch (IOException ex) {
      throw new DataIngestException("Unable to read next page");
    }
  }
}
