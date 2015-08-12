package org.apache.parquet.parqour.ingest.disk.pages.meta;

import org.apache.parquet.format.PageHeader;
import org.apache.parquet.parqour.ingest.disk.pages.PageInfo;

/**
 * Created by sircodesalot on 8/11/15.
 */
public class PageMeta {
  private final PageHeader header;
  private final PageInfo pageInfo;

  public PageMeta(PageHeader header, PageInfo pageInfo) {
    this.header = header;
    this.pageInfo = pageInfo;
  }

  public long totalEntryCount() { return pageInfo.entryCount(); }
  public PageInfo pageInfo() { return pageInfo; }
}
