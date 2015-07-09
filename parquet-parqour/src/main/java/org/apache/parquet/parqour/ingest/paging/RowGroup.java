package org.apache.parquet.parqour.ingest.paging;

import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;

/**
 * Alias for PageReadStore because I have to keep checking what that name means.
 */
public class RowGroup {
  private final PageReadStore group;

  public RowGroup(PageReadStore group) {
    this.group = group;
  }

  public PageReader readPageForColumn(ColumnDescriptor column) {
    if (group == null) {
      throw new DataIngestException("Read past end of file");
    }

    return group.getPageReader(column);
  }
}
