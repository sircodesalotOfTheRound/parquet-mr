package org.apache.parquet.parqour.ingest.paging;

import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DictionaryPage;

/**
 * Created by sircodesalot on 7/14/15.
 */
public class PagePair {
  private final DataPage page;
  private final DictionaryPage dictionaryPage;
  private final long totalItems;

  public PagePair(DataPage page, DictionaryPage dictionaryPage, long totalItems) {
    this.page = page;
    this.dictionaryPage = dictionaryPage;
    this.totalItems = totalItems;
  }

  public DataPage page() { return this.page; }
  public DictionaryPage dictionaryPage() { return this.dictionaryPage; }
  public long totalItems() { return this.totalItems; }
}
