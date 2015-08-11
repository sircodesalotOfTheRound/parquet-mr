package org.apache.parquet.parqour.ingest.disk.pages;

import org.apache.parquet.parqour.ingest.disk.pages.meta.pagemetas.PageMeta;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.FastForwardReader;

/**
 * Created by sircodesalot on 8/12/15.
 */
public class Page {
  private final PageMeta pageMeta;
  private final long firstEntry;

  public Page(PageMeta pageMeta, long firstEntry)  {
    this.pageMeta = pageMeta;
    this.firstEntry = firstEntry;
  }

  public long firstEntry() { return this.firstEntry; }
  public long totalEntries() { return this.pageMeta.totalEntryCount(); }

  public boolean containsEntry(long entryNumber) {
    return (entryNumber >= firstEntry) && (entryNumber < (firstEntry + totalEntries()));
  }

  public <T extends FastForwardReader> T repetitionLevelReader() { return pageMeta.repetitionLevelReader(); }
  public <T extends FastForwardReader> T definitionLevelReader() { return pageMeta.definitionLevelReader(); }
  public <T extends FastForwardReader> T contentReader() { return pageMeta.contentReader(); }
}
