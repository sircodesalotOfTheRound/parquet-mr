package org.apache.parquet.parqour.ingest.disk.pages;

import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.ingest.disk.blocks.RowGroupColumnInfo;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFile;

/**
 * Created by sircodesalot on 8/10/15.
 */
public class DictionaryPageInfo extends PageInfo {
  private final DictionaryPageHeader pageHeader;

  public DictionaryPageInfo(RowGroupColumnInfo columnInfo, HDFSParquetFile file, PageHeader header) {
    super(columnInfo, file, header);
    this.pageHeader = header.getDictionary_page_header();
  }

  @Override
  public boolean isDictionaryPage() {
    return true;
  }

  @Override
  public long entryCount() {
    return pageHeader.getNum_values();
  }
}
