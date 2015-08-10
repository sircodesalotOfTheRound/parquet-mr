package org.apache.parquet.parqour.ingest.disk.pages;

import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFile;

/**
 * Created by sircodesalot on 8/10/15.
 */
public class DictionaryPageInfo extends PageInfo {
  private final DictionaryPageHeader pageHeader;

  public DictionaryPageInfo(HDFSParquetFile file, PageHeader header) {
    super(file, header);

    this.pageHeader = header.getDictionary_page_header();
  }

  @Override
  public ParquetProperties.WriterVersion version() {
    throw new DataIngestException("Dictionary pages do not have version numbers.");
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
