package org.apache.parquet.parqour.ingest.disk.pages.meta.pagemetas;

import org.apache.parquet.column.ValuesType;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.parqour.ingest.disk.pages.info.DataPageInfo;
import org.apache.parquet.parqour.ingest.ffreader.FastForwardReaderBase;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.FastForwardReader;

/**
 * Created by sircodesalot on 8/11/15.
 */
public class PageMeta {
  private final PageHeader header;
  private final DataPageInfo pageInfo;

  public PageMeta(PageHeader header, DataPageInfo pageInfo) {
    this.header = header;
    this.pageInfo = pageInfo;
  }

  public long totalEntryCount() { return pageInfo.entryCount(); }

  public <T extends FastForwardReader> T repetitionLevelReader() {
    return (T)FastForwardReaderBase.resolve(pageInfo, ValuesType.REPETITION_LEVEL);
  }

  public <T extends FastForwardReader> T definitionLevelReader() {
    return (T)FastForwardReaderBase.resolve(pageInfo, ValuesType.DEFINITION_LEVEL);
  }

  public <T extends FastForwardReader> T contentReader() {
    return (T)FastForwardReaderBase.resolve(pageInfo, ValuesType.VALUES);
  }
}
