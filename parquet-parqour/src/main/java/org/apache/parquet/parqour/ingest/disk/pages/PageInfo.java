package org.apache.parquet.parqour.ingest.disk.pages;

import org.apache.parquet.format.PageHeader;
import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.ingest.disk.blocks.RowGroupColumnInfo;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFile;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFileMetadata;

/**
 * Created by sircodesalot on 8/10/15.
 */
public abstract class PageInfo {
  protected final RowGroupColumnInfo columnInfo;
  protected final HDFSParquetFile file;
  protected final PageHeader header;

  public PageInfo(RowGroupColumnInfo columnInfo, HDFSParquetFile file, PageHeader header) {
    this.columnInfo = columnInfo;
    this.file = file;
    this.header = header;
  }

  public abstract boolean isDictionaryPage();
  public abstract long entryCount();

  public static PageInfo readPage(RowGroupColumnInfo columnInfo, HDFSParquetFile file, HDFSParquetFileMetadata metadata, PageHeader header) {
    switch (header.getType()) {
      case DATA_PAGE:
        return new PageInfoV1(columnInfo, file, metadata, header);
      case DATA_PAGE_V2:
        return new PageInfoV2(columnInfo, file, metadata, header);
      case DICTIONARY_PAGE:
        return new DictionaryPageInfo(columnInfo, file, header);
    }

    throw new DataIngestException("Invalid page type");
  }
}
