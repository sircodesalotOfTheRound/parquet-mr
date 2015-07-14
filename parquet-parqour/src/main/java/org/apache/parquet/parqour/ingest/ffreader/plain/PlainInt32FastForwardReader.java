package org.apache.parquet.parqour.ingest.ffreader.plain;

import org.apache.parquet.column.ValuesType;
import org.apache.parquet.parqour.ingest.ffreader.FastForwardReaderBase;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.Int32FastForwardReader;
import org.apache.parquet.parqour.ingest.paging.DataPageMetadata;

/**
 * Created by sircodesalot on 6/13/15.
 */
public final class PlainInt32FastForwardReader extends FastForwardReaderBase implements Int32FastForwardReader {
  private static final int SIZEOF_INT32 = 4;

  public PlainInt32FastForwardReader(DataPageMetadata metadata, ValuesType type) {
    super(metadata, type);
  }

  @Override
  public int readi32() {
    super.advanceRowNumber();

    return (data[++dataOffset] & 0xFF)
      | (data[++dataOffset] & 0xFF) << 8
      | (data[++dataOffset] & 0xFF) << 16
      | (data[++dataOffset] & 0xFF) << 24;
  }

  @Override
  public void fastForwardTo(int entryNumber) {
    long jumpDistance = (entryNumber - currentEntryNumber) * SIZEOF_INT32;

    dataOffset += jumpDistance;
    currentEntryNumber = entryNumber;
  }
}
