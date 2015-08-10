package org.apache.parquet.parqour.ingest.disk.pages;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.parqour.ingest.disk.blocks.RowGroupColumnInfo;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFile;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFileMetadata;
import org.apache.parquet.parqour.ingest.disk.pages.slate.DataSlate;

/**
 * Created by sircodesalot on 8/10/15.
 */
public class PageInfoV1 extends DataPageInfo {
  private final ParquetMetadataConverter converter = new ParquetMetadataConverter();
  private final DataPageHeader pageHeader;
  private final HDFSParquetFileMetadata metadata;

  public PageInfoV1(RowGroupColumnInfo columnInfo, HDFSParquetFileMetadata metadata, PageHeader header, DataSlate slate, long offset) {
    super(columnInfo, metadata, header, slate, offset);

    this.metadata = metadata;
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

  @Override
  public Encoding definitionLevelEncoding() {
    return converter.getEncoding(pageHeader.getDefinition_level_encoding());
  }

  @Override
  public Encoding repetitionLevelEncoding() {
    return converter.getEncoding(pageHeader.getRepetition_level_encoding());
  }

  @Override
  public Encoding dataEncoding() {
    return converter.getEncoding(pageHeader.getEncoding());
  }

  @Override
  public Statistics statistics() {
    return ParquetMetadataConverter.fromParquetStatistics(
      metadata.createdBy(),
      pageHeader.getStatistics(),
      super.columnDescriptor.getType());
  }
}
