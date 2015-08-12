package org.apache.parquet.parqour.ingest.disk.pages;

import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.parqour.ingest.disk.pagesets.RowGroupPageSetColumnInfo;
import org.apache.parquet.parqour.ingest.disk.pages.slate.DataSlate;

/**
 * Created by sircodesalot on 8/10/15.
 */
public class DictionaryPageInfo extends PageInfo {
  private final DictionaryPageHeader pageHeader;

  public DictionaryPageInfo(RowGroupPageSetColumnInfo columnInfo, PageHeader header, DataSlate slate, int offset) {
    super(columnInfo, header, slate, offset);
    this.pageHeader = header.getDictionary_page_header();
  }

  @Override
  public boolean isDictionaryPage() {
    return true;
  }

  public int startingOffset() { return super.offset; }

  @Override
  public long entryCount() {
    return pageHeader.getNum_values();
  }
}
