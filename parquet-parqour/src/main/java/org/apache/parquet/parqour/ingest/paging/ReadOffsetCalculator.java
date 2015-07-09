package org.apache.parquet.parqour.ingest.paging;


import org.apache.parquet.column.ColumnDescriptor;

/**
 * Created by sircodesalot on 6/15/15.
 */
public class ReadOffsetCalculator {
  private static final int SIZEOF_INT32 = 4;

  private final int definitionLevelOffset;
  private final int repetitionLevelOffset;
  private final int contentOffset;

  public ReadOffsetCalculator(DataPageVersion pageVersion, byte[] data, ColumnDescriptor descriptor) {
    this.repetitionLevelOffset = computeRepetitionLevelOffset();
    this.definitionLevelOffset = computeDefinitionLevelOffset(pageVersion, data, descriptor);
    this.contentOffset = computeContentOffset(pageVersion, data, descriptor);
  }

  private int computeRepetitionLevelOffset() {
    return 0;
  }

  private int computeDefinitionLevelOffset(DataPageVersion pageVersion, byte[] data, ColumnDescriptor descriptor) {
    if (pageVersion != DataPageVersion.DATA_PAGE_VERSION_2_0) {
      return computeRepetitionLevelLength(data, descriptor);
    } else {
      return 0;
    }
  }

  private int computeContentOffset(DataPageVersion pageVersion, byte[] data, ColumnDescriptor descriptor) {
    if (pageVersion != DataPageVersion.DATA_PAGE_VERSION_2_0) {
      return computeRepetitionLevelLength(data, descriptor) + computeDefinitionLevelLength(data, descriptor);
    } else {
      return 0;
    }
  }

  private int computeDefinitionLevelLength(byte[] data, ColumnDescriptor descriptor) {
    if (descriptor.getMaxDefinitionLevel() != 0) {
      int repetitionLevelLength = computeRepetitionLevelLength(data, descriptor);
      return readLength(data, repetitionLevelLength) + SIZEOF_INT32;
    } else {
      return 0;
    }
  }

  private int computeRepetitionLevelLength(byte[] data, ColumnDescriptor descriptor) {
    if (descriptor.getMaxRepetitionLevel() != 0) {
      return readLength(data, 0) + SIZEOF_INT32;
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
