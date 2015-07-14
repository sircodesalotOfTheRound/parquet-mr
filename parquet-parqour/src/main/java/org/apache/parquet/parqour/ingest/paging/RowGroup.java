package org.apache.parquet.parqour.ingest.paging;

import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;

import java.util.Map;

/**
 * Alias for PageReadStore because I have to keep checking what that name means.
 */
public class RowGroup {
  private final PageReadStore group;
  private final ColumnDescriptor forColumn;
  private PagePair nextPagePair;

  public RowGroup(ColumnDescriptor forColumn, PageReadStore group) {
    this.forColumn = forColumn;
    this.group = group;
    this.nextPagePair = readNextPagePair();
  }

  private PagePair readNextPagePair() {
    PageReader pageReader = group.getPageReader(forColumn);
    long totalItems = pageReader.getTotalValueCount();
    DataPage page = pageReader.readPage();
    DictionaryPage dictionaryPage = pageReader.readDictionaryPage();

    if (page != null) {
      return new PagePair(page, dictionaryPage, totalItems);
    } else {
      return null;
    }
  }

  public PagePair getNextPage() {
    if (nextPagePair == null) {
      throw new DataIngestException("No more pages to read in this row-group.");
    }

    PagePair nextPagePair = this.nextPagePair;
    this.nextPagePair = readNextPagePair();
    return nextPagePair;
  }

  boolean hasMorePages() { return this.nextPagePair != null; }
}
