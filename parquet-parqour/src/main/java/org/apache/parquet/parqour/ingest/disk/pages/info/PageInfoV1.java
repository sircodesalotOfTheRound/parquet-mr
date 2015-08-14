package org.apache.parquet.parqour.ingest.disk.pages.info;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.parqour.ingest.disk.pagesets.RowGroupPageSetColumnInfo;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFileMetadata;
import org.apache.parquet.parqour.ingest.disk.pages.slate.DataSlate;
import org.apache.parquet.parqour.ingest.paging.ReadOffsetCalculator;

/**
 * Created by sircodesalot on 8/10/15.
 */
public class PageInfoV1 extends DataPageInfo {
  private final ParquetMetadataConverter converter = new ParquetMetadataConverter();
  private final DataPageHeader pageHeader;
  protected ReadOffsetCalculator calculator;
  private final HDFSParquetFileMetadata metadata;

  public PageInfoV1(RowGroupPageSetColumnInfo columnInfo, HDFSParquetFileMetadata metadata, PageHeader header,
                    DataSlate slate, DictionaryPageInfo dictionaryPage) {

    super(columnInfo, metadata, header, slate, dictionaryPage);

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
  public int entryCount() {
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
  public Encoding contentEncoding() {
    return converter.getEncoding(pageHeader.getEncoding());
  }

  public int definitionLevelOffset() { return offsetCalculator().definitionLevelOffset(); }
  public int repetitionLevelOffset() { return offsetCalculator().repetitionLevelOffset(); }
  public int contentOffset() { return offsetCalculator().contentOffset(); }

  private ReadOffsetCalculator offsetCalculator() {
    if (this.calculator == null) {
      this.calculator = new ReadOffsetCalculator(version(), data(), columnDescriptor(), offset);
    }

    return this.calculator;
  }

  @Override
  public Statistics statistics() {
    return ParquetMetadataConverter.fromParquetStatistics(
      metadata.createdBy(),
      pageHeader.getStatistics(),
      super.columnDescriptor.getType());
  }

  @Override
  public byte[] repetitionLevelData() {
    return super.data();
  }

  @Override
  public byte[] definitionLevelData() {
    return super.data();
  }
}
