package org.apache.parquet.parqour.ingest.paging;


import org.apache.parquet.column.ColumnDescriptor;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by sircodesalot on 6/15/15.
 */
public class PageCache {
  private final ColumnDescriptor column;
  private final List<DataPageDecorator> pages;

  public PageCache(ColumnDescriptor column) {
    this.column = column;
    this.pages = new ArrayList<DataPageDecorator>();
  }

  public DataPageDecorator getPageByIndex(int index) {
    return pages.get(index);
  }
}
