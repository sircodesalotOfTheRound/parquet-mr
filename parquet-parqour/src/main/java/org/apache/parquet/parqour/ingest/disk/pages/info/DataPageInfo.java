package org.apache.parquet.parqour.ingest.disk.pages.info;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ValuesType;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFileMetadata;
import org.apache.parquet.parqour.ingest.disk.pages.slate.DataSlate;
import org.apache.parquet.parqour.ingest.disk.pagesets.RowGroupPageSetColumnInfo;
import org.apache.parquet.parqour.ingest.paging.ReadOffsetCalculator;
import org.apache.parquet.schema.PrimitiveType;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Created by sircodesalot on 8/10/15.
 */
public abstract class DataPageInfo extends PageInfo {
  protected ColumnDescriptor columnDescriptor;
  protected final DictionaryPageInfo dictionaryPage;

  public DataPageInfo(RowGroupPageSetColumnInfo columnInfo, HDFSParquetFileMetadata metadata,
                      PageHeader header, DataSlate slate, DictionaryPageInfo dictionaryPage) {
    super(columnInfo, header, slate);

    this.columnDescriptor = metadata.getColumnDescriptor(columnInfo.path());
    this.dictionaryPage = dictionaryPage;
  }


  public abstract ParquetProperties.WriterVersion version();

  public abstract Encoding definitionLevelEncoding();
  public abstract Encoding repetitionLevelEncoding();
  public abstract Encoding contentEncoding();
  public abstract Statistics statistics();


  public abstract byte[] repetitionLevelData();
  public abstract byte[] definitionLevelData();

  public PrimitiveType.PrimitiveTypeName type() { return columnInfo.type(); }

  public abstract int definitionLevelOffset();
  public abstract int repetitionLevelOffset();
  public abstract int contentOffset();

  public DictionaryPageInfo dictionaryPage() { return this.dictionaryPage; }

  public int computeOffset(ValuesType type) {
    switch (type) {
      case REPETITION_LEVEL:
        return repetitionLevelOffset();
      case DEFINITION_LEVEL:
        return definitionLevelOffset();
      case VALUES:
        return contentOffset();
    }

    throw new NotImplementedException();
  }

  public int repetitionLevel() { return columnDescriptor.getMaxRepetitionLevel(); }
  public int definitionLevel() { return columnDescriptor.getMaxDefinitionLevel(); }
  public int typeLength() { return columnDescriptor.getTypeLength(); }

  public int relationshipLevelForType(ValuesType type) {
    switch (type) {
      case REPETITION_LEVEL:
        return this.repetitionLevel();
      case DEFINITION_LEVEL:
        return this.definitionLevel();
    }

    throw new NotImplementedException();
  }

  public byte[] dataForType(ValuesType type) {
    switch (type) {
      case REPETITION_LEVEL:
        return repetitionLevelData();
      case DEFINITION_LEVEL:
        return definitionLevelData();
      case VALUES:
        return data();
    }

    throw new NotImplementedException();
  }

  @Override
  public boolean isDictionaryPage() { return false; }

  public ColumnDescriptor columnDescriptor() { return this.columnDescriptor; }
}
