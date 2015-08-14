package org.apache.parquet.parqour.ingest.disk.pages.info;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.parqour.ingest.disk.pagesets.RowGroupPageSetColumnInfo;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFileMetadata;
import org.apache.parquet.parqour.ingest.disk.pages.slate.DataSlate;

/**
 * Created by sircodesalot on 8/10/15.
 */
public class PageInfoV2 extends DataPageInfo {
  public static ParquetMetadataConverter converter = new ParquetMetadataConverter();
  private final DataPageHeaderV2 pageHeader;
  private final HDFSParquetFileMetadata metadata;

  // TODO: Separate this
  private byte[] repetitionLevelData;
  private byte[] definitionLevelData;

  private final int repetitionLevelOffset;
  private final int definitionLevelOffset;
  private final int contentOffset;

  public PageInfoV2(RowGroupPageSetColumnInfo columnInfo, HDFSParquetFileMetadata metadata, PageHeader header,
                    DataSlate slate, DictionaryPageInfo dictionaryPage) {
    super(columnInfo, metadata, header, slate, dictionaryPage);

    this.pageHeader = header.getData_page_header_v2();
    this.metadata = metadata;

    this.repetitionLevelOffset = computeRepetitionLevelOffset(header, pageHeader);
    this.definitionLevelOffset = computeDefinitionLevelOffset(header, pageHeader);
    this.contentOffset = computeContentOffset(header, pageHeader);
  }

  private int computeRepetitionLevelOffset(PageHeader header, DataPageHeaderV2 pageHeader) {
    return offset;
  }

  private int computeDefinitionLevelOffset(PageHeader header, DataPageHeaderV2 pageHeader) {
    return computeRepetitionLevelOffset(header, pageHeader) + pageHeader.getRepetition_levels_byte_length();
  }

  private int computeContentOffset(PageHeader header, DataPageHeaderV2 pageHeader) {
    return computeDefinitionLevelOffset(header, pageHeader) + pageHeader.getDefinition_levels_byte_length();
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
  public int entryCount() {
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
  public Encoding contentEncoding() {
    return converter.getEncoding(pageHeader.getEncoding());
  }

  @Override
  public Statistics statistics() {
    return ParquetMetadataConverter.fromParquetStatistics(
      metadata.createdBy(),
      pageHeader.getStatistics(),
      super.columnDescriptor.getType());
  }

  public int definitionLevelOffset() { return definitionLevelOffset; }
  public int repetitionLevelOffset() { return repetitionLevelOffset; }
  public int contentOffset() { return contentOffset; }

  @Override
  public byte[] repetitionLevelData() {
    return data();
  }

  @Override
  public byte[] definitionLevelData() {
    return data();
  }
}
