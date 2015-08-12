package org.apache.parquet.parqour.ingest.disk.pages.info;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ValuesType;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.parqour.ingest.disk.pagesets.RowGroupPageSetColumnInfo;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFileMetadata;
import org.apache.parquet.parqour.ingest.disk.pages.slate.DataSlate;
import org.apache.parquet.parqour.ingest.paging.ReadOffsetCalculator;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Created by sircodesalot on 8/10/15.
 */
public abstract class DataPageInfo extends PageInfo {
  protected ColumnDescriptor columnDescriptor;
  protected ReadOffsetCalculator calculator;

  public DataPageInfo(RowGroupPageSetColumnInfo columnInfo, HDFSParquetFileMetadata metadata, PageHeader header, DataSlate slate, int offset) {
    super(columnInfo, header, slate, offset);

    this.columnDescriptor = metadata.getColumnDescriptor(columnInfo.path());
  }

  private ReadOffsetCalculator offsetCalculator() {
    if (this.calculator == null) {
      this.calculator = new ReadOffsetCalculator(version(), data(), columnDescriptor(), offset);
    }

    return this.calculator;
  }

  public abstract ParquetProperties.WriterVersion version();

  public abstract Encoding definitionLevelEncoding();
  public abstract Encoding repetitionLevelEncoding();
  public abstract Encoding dataEncoding();
  public abstract Statistics statistics();

  public int definitionLevelOffset() { return offsetCalculator().definitionLevelOffset(); }
  public int repetitionLevelOffset() { return offsetCalculator().repetitionLevelOffset(); }
  public int contentOffset() { return offsetCalculator().contentOffset(); }

  public int computeOffset(ValuesType type) {
    switch (type) {
      case REPETITION_LEVEL:
        return offsetCalculator().repetitionLevelOffset();
      case DEFINITION_LEVEL:
        return offsetCalculator().definitionLevelOffset();
      case VALUES:
        return offsetCalculator().contentOffset();
    }

    throw new NotImplementedException();
  }

  @Override
  public boolean isDictionaryPage() { return false; }

  public ColumnDescriptor columnDescriptor() { return this.columnDescriptor; }
}
