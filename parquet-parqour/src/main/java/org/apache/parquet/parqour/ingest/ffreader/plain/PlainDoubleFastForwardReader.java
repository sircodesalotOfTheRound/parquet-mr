package org.apache.parquet.parqour.ingest.ffreader.plain;

import org.apache.parquet.column.ValuesType;
import org.apache.parquet.parqour.ingest.disk.pages.info.DataPageInfo;
import org.apache.parquet.parqour.ingest.ffreader.FastForwardReaderBase;
import org.apache.parquet.parqour.ingest.paging.DataPageMetadata;

/**
 * Created by sircodesalot on 6/13/15.
 */
public final class PlainDoubleFastForwardReader extends FastForwardReaderBase {
  private static final int SIZEOF_DOUBLE = 8;

  @Deprecated
  public PlainDoubleFastForwardReader(DataPageMetadata metadata, ValuesType type) {
    super(metadata, type);
  }

  public PlainDoubleFastForwardReader(DataPageInfo info, ValuesType values) {
    super(info, values);
  }

  public double readDouble() {
    super.advanceEntryNumber();

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
    long jumpDistance = (entryNumber - currentEntryNumber) * SIZEOF_DOUBLE;

    dataOffset += jumpDistance;
    currentEntryNumber = entryNumber;
  }
}

