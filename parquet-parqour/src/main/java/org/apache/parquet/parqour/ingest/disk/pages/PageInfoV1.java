package org.apache.parquet.parqour.ingest.disk.pages;

import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFile;

/**
 * Created by sircodesalot on 8/10/15.
 */
public class PageInfoV1 extends PageInfo {
  private final DataPageHeader pageHeader;

  public PageInfoV1(HDFSParquetFile file, PageHeader header) {
    super(file, header);

    this.pageHeader = header.getData_page_header();
  }

  @Override
  public ParquetProperties.WriterVersion version() {
    return ParquetProperties.WriterVersion.PARQUET_1_0;
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
