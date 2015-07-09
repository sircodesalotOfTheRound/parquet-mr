package org.apache.parquet.parqour.testtools;


import org.apache.parquet.column.ParquetProperties;

import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;

/**
 * Created by sircodesalot on 6/25/15.
 */
public enum ParquetConfiguration {
  V1_NO_DICTIONARY(PARQUET_1_0, false),
  V1_WITH_DICTIONARY(PARQUET_1_0, true),
  V2_NO_DICTIONARY(PARQUET_2_0, false),
  V2_WITH_DICTIONARY(PARQUET_2_0, true);

  private final ParquetProperties.WriterVersion version;
  private final boolean useDictionary;

  ParquetConfiguration(ParquetProperties.WriterVersion version, boolean useDictionary) {
    this.version = version;
    this.useDictionary = useDictionary;
  }

  public ParquetProperties.WriterVersion version() { return this.version; }
  public boolean useDictionary() { return this.useDictionary; }

  @Override
  public String toString() {
    return String.format("Version: %s, Use Dictionary: %s", version, useDictionary);
  }
}
