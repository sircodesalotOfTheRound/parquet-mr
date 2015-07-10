package org.apache.parquet.parqour.ingest.ffreader.plain;

import org.apache.parquet.parqour.ingest.ffreader.FastForwardReaderBase;
import org.apache.parquet.parqour.ingest.paging.DataPageMetadata;
import org.apache.parquet.column.ValuesType;

/**
 * Created by sircodesalot on 6/13/15.
 */
public final class PlainSingleFastForwardReader extends FastForwardReaderBase {
  public PlainSingleFastForwardReader(DataPageMetadata metadata, ValuesType type) {
    super(metadata, type);
  }

  public float readSingle() {
    super.advanceRowNumber();

    int rawIntBits = ((int)data[++dataOffset] & 0xFF)
      | ((int)data[++dataOffset] & 0xFF) << 8
      | ((int)data[++dataOffset] & 0xFF) << 16
      | ((int)data[++dataOffset] & 0xFF) << 24;

    return Float.intBitsToFloat(rawIntBits);
  }

  @Override
  public void fastForwardTo(int entryNumber) {

  }
}

