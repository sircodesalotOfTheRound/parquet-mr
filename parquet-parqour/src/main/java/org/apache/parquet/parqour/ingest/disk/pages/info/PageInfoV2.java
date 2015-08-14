package org.apache.parquet.parqour.ingest.disk.pages.info;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.ingest.disk.pagesets.RowGroupPageSetColumnInfo;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFileMetadata;
import org.apache.parquet.parqour.ingest.disk.pages.slate.DataSlate;

import java.io.IOException;

/**
 * Created by sircodesalot on 8/10/15.
 */
public class PageInfoV2 extends DataPageInfo {
  public static ParquetMetadataConverter converter = new ParquetMetadataConverter();
  private final DataPageHeaderV2 pageHeader;
  private final HDFSParquetFileMetadata metadata;

  private byte[] repetitionLevelData;
  private byte[] definitionLevelData;

  private final int repetitionLevelOffset;
  private final int definitionLevelOffset;

  public PageInfoV2(RowGroupPageSetColumnInfo columnInfo, HDFSParquetFileMetadata metadata, PageHeader header,
                    DataSlate slate, DictionaryPageInfo dictionaryPage) {
    super(columnInfo, metadata, header, slate, dictionaryPage);

    this.pageHeader = header.getData_page_header_v2();
    this.metadata = metadata;

    this.repetitionLevelOffset = addSegment(pageHeader.getRepetition_levels_byte_length());
    this.definitionLevelOffset = addSegment(pageHeader.getDefinition_levels_byte_length());
  }

  private int addSegment(int length) {
    return slate.addSegment(length);
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

  @Override
  public byte[] repetitionLevelData() {
    return data();
  }

  @Override
  public byte[] definitionLevelData() {
    return data();
  }
}
