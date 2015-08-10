package org.apache.parquet.parqour.ingest.disk.pages;

import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFile;

/**
 * Created by sircodesalot on 8/10/15.
 */
public class PageInfoV2 extends PageInfo {
  private final DataPageHeaderV2 pageHeader;

  public PageInfoV2(HDFSParquetFile file, PageHeader header) {
    super(file, header);

    this.pageHeader = header.getData_page_header_v2();
  }

  @Override
  public ParquetProperties.WriterVersion version() {
    return ParquetProperties.WriterVersion.PARQUET_2_0;
  }

  @Override
  public boolean isDictionaryPage() {
    return false;
  }

  @Override
  public long entryCount() {
    return pageHeader.getNum_values();
  }
}
