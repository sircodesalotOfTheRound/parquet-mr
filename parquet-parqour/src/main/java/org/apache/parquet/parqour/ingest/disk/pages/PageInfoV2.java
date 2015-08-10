package org.apache.parquet.parqour.ingest.disk.pages;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.parqour.ingest.disk.blocks.RowGroupColumnInfo;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFile;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFileMetadata;
import org.apache.parquet.parqour.ingest.paging.ReadOffsetCalculator;

/**
 * Created by sircodesalot on 8/10/15.
 */
public class PageInfoV2 extends DataPageInfo {
  public static ParquetMetadataConverter converter = new ParquetMetadataConverter();
  private final DataPageHeaderV2 pageHeader;
  private final HDFSParquetFileMetadata metadata;

  public PageInfoV2(RowGroupColumnInfo columnInfo, HDFSParquetFile file, HDFSParquetFileMetadata metadata, PageHeader header) {
    super(columnInfo, file, metadata, header);

    this.pageHeader = header.getData_page_header_v2();
    this.metadata = metadata;
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

  @Override
  public Encoding definitionLevelEncoding() {
    return Encoding.RLE;
  }

  @Override
  public Encoding repetitionLevelEncoding() {
    return Encoding.RLE;
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
