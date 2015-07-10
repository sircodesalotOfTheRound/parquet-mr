package org.apache.parquet.parqour.ingest.ffreader.plain;

import org.apache.parquet.parqour.ingest.ffreader.FastForwardReaderBase;
import org.apache.parquet.parqour.ingest.paging.DataPageMetadata;
import org.apache.parquet.column.ValuesType;

/**
 * Created by sircodesalot on 6/13/15.
 */
public final class PlainDoubleFastForwardReader extends FastForwardReaderBase {
  public PlainDoubleFastForwardReader(DataPageMetadata metadata, ValuesType type) {
    super(metadata, type);
  }

  public double readDouble() {
    super.advanceRowNumber();

    long rawLongBits = ((long)data[++dataOffset] & 0xFF)
      | ((long)data[++dataOffset] & 0xFF) << 8
      | ((long)data[++dataOffset] & 0xFF) << 16
      | ((long)data[++dataOffset] & 0xFF) << 24
      | ((long)data[++dataOffset] & 0xFF) << 32
      | ((long)data[++dataOffset] & 0xFF) << 40
      | ((long)data[++dataOffset] & 0xFF) << 48
      | ((long)data[++dataOffset] & 0xFF) << 56;

    return Double.longBitsToDouble(rawLongBits);
  }

  @Override
  public void fastForwardTo(int entryNumber) {

  }
}

