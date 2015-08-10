package org.apache.parquet.parqour.ingest.disk.pages;

import org.apache.parquet.format.PageHeader;
import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.ingest.disk.blocks.RowGroupColumnInfo;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFile;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFileMetadata;
import org.apache.parquet.parqour.ingest.disk.pages.slate.DataSlate;

/**
 * Created by sircodesalot on 8/10/15.
 */
public abstract class PageInfo {
  protected final RowGroupColumnInfo columnInfo;
  protected final PageHeader header;
  protected final DataSlate slate;
  protected final long offset;

  public PageInfo(RowGroupColumnInfo columnInfo, PageHeader header, DataSlate slate, long offset) {
    this.columnInfo = columnInfo;
    this.header = header;
    this.slate = slate;
    this.offset = offset;
  }

  public abstract boolean isDictionaryPage();
  public abstract long entryCount();

  public static PageInfo readPage(RowGroupColumnInfo columnInfo, HDFSParquetFile file, HDFSParquetFileMetadata metadata,
                                  PageHeader header, DataSlate slate, long offset) {
    switch (header.getType()) {
      case DATA_PAGE:
        return new PageInfoV1(columnInfo, metadata, header, slate, offset);
      case DATA_PAGE_V2:
        return new PageInfoV2(columnInfo, metadata, header, slate, offset);
      case DICTIONARY_PAGE:
        return new DictionaryPageInfo(columnInfo, header, slate, offset);
    }

    throw new DataIngestException("Invalid page type");
  }
}
