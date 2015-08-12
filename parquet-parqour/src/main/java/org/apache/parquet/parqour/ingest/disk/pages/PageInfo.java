package org.apache.parquet.parqour.ingest.disk.pages;

import org.apache.parquet.format.PageHeader;
import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.ingest.disk.pagesets.RowGroupPageSetColumnInfo;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFileMetadata;
import org.apache.parquet.parqour.ingest.disk.pages.slate.DataSlate;

/**
 * Created by sircodesalot on 8/10/15.
 */
public abstract class PageInfo {
  protected final RowGroupPageSetColumnInfo columnInfo;
  protected final PageHeader header;
  protected final DataSlate slate;
  protected final int offset;

  public PageInfo(RowGroupPageSetColumnInfo columnInfo, PageHeader header, DataSlate slate, int offset) {
    this.columnInfo = columnInfo;
    this.header = header;
    this.slate = slate;
    this.offset = offset;
  }

  public abstract boolean isDictionaryPage();
  public abstract long entryCount();

  public byte[] data() {
    return slate.data();
  }

  public static PageInfo readPage(RowGroupPageSetColumnInfo columnInfo, HDFSParquetFileMetadata metadata, PageHeader header,
                                  DataSlate slate, int offset) {
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
