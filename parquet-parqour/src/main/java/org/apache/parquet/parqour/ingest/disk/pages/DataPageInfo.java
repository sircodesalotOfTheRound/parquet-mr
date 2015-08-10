package org.apache.parquet.parqour.ingest.disk.pages;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.parqour.ingest.disk.blocks.RowGroupColumnInfo;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFileMetadata;
import org.apache.parquet.parqour.ingest.disk.pages.slate.DataSlate;

/**
 * Created by sircodesalot on 8/10/15.
 */
public abstract class DataPageInfo extends PageInfo {
  protected ColumnDescriptor columnDescriptor;

  public DataPageInfo(RowGroupColumnInfo columnInfo, HDFSParquetFileMetadata metadata, PageHeader header, DataSlate slate, long offset) {
    super(columnInfo, header, slate, offset);

    this.columnDescriptor = metadata.getColumnDescriptor(columnInfo.path());
  }

  public abstract ParquetProperties.WriterVersion version();

  public abstract Encoding definitionLevelEncoding();
  public abstract Encoding repetitionLevelEncoding();
  public abstract Encoding dataEncoding();
  public abstract Statistics statistics();

  @Override
  public boolean isDictionaryPage() { return false; }

  public ColumnDescriptor columnDescriptor() { return this.columnDescriptor; }
}
