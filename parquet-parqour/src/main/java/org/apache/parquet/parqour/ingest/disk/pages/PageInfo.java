package org.apache.parquet.parqour.ingest.disk.pages;

import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.ingest.disk.blocks.RowGroupColumnInfo;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFile;

/**
 * Created by sircodesalot on 8/10/15.
 */
public abstract class PageInfo {
  private final HDFSParquetFile file;
  private final PageHeader header;

  public PageInfo(HDFSParquetFile file, PageHeader header) {
    this.file = file;
    this.header = header;
  }

  public abstract ParquetProperties.WriterVersion version();
  public abstract boolean isDictionaryPage();
  public abstract long entryCount();

  public static PageInfo readPage(HDFSParquetFile file, PageHeader header) {
    switch (header.getType()) {
      case DATA_PAGE:
        return new PageInfoV1(file, header);
      case DATA_PAGE_V2:
        return new PageInfoV2(file, header);
      case DICTIONARY_PAGE:
        return new DictionaryPageInfo(file, header);
    }

    throw new DataIngestException("Invalid page type");
  }
}
