package org.apache.parquet.parqour.ingest.paging;


import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;

/**
 * Created by sircodesalot on 6/15/15.
 */
public class ReadOffsetCalculator {
  private static final int SIZEOF_INT32 = 4;

  private final int definitionLevelOffset;
  private final int repetitionLevelOffset;
  private final int contentOffset;

  public ReadOffsetCalculator(ParquetProperties.WriterVersion version, byte[] data, ColumnDescriptor descriptor, int startOffset) {
    this.repetitionLevelOffset = computeRepetitionLevelOffset(startOffset);
    this.definitionLevelOffset = computeDefinitionLevelOffset(version, data, descriptor, startOffset);
    this.contentOffset = computeContentOffset(version, data, descriptor, startOffset);
  }

  private int computeRepetitionLevelOffset(int startOffset) {
    return startOffset;
  }

  private int computeDefinitionLevelOffset(ParquetProperties.WriterVersion pageVersion, byte[] data, ColumnDescriptor descriptor, int startOffset) {
    if (pageVersion != ParquetProperties.WriterVersion.PARQUET_2_0) {
      return startOffset + computeRepetitionLevelLength(data, descriptor, startOffset);
    } else {
      return startOffset;
    }
  }

  private int computeContentOffset(ParquetProperties.WriterVersion pageVersion, byte[] data, ColumnDescriptor descriptor, int startOffset) {
    if (pageVersion != ParquetProperties.WriterVersion.PARQUET_2_0) {
      return startOffset
        + computeRepetitionLevelLength(data, descriptor, startOffset)
        + computeDefinitionLevelLength(data, descriptor, startOffset);
    } else {
      return startOffset;
    }
  }

  private int computeDefinitionLevelLength(byte[] data, ColumnDescriptor descriptor, int startOffset) {
    if (descriptor.getMaxDefinitionLevel() != 0) {
      int repetitionLevelLength = computeRepetitionLevelLength(data, descriptor, startOffset);
      return readLength(data, startOffset + repetitionLevelLength) + SIZEOF_INT32;
    } else {
      return 0;
    }
  }

  private int computeRepetitionLevelLength(byte[] data, ColumnDescriptor descriptor, int startOffset) {
    if (descriptor.getMaxRepetitionLevel() != 0) {
      return readLength(data, startOffset) + SIZEOF_INT32;
    } else {
      return 0;
    }
  }

  private int readLength(byte[] data, int offset) {
    return (data[offset++] & 0xFF)
      | (data[offset++] & 0xFF) << 8
      | (data[offset++] & 0xFF) << 16
      | (data[offset] & 0xFF) << 24;
  }

  public int definitionLevelOffset() { return this.definitionLevelOffset; }
  public int repetitionLevelOffset() { return this.repetitionLevelOffset; }
  public int contentOffset() { return this.contentOffset; }
}
