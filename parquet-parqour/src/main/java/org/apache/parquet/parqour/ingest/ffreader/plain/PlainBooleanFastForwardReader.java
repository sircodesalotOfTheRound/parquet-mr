package org.apache.parquet.parqour.ingest.ffreader.plain;

import org.apache.parquet.column.ValuesType;
import org.apache.parquet.parqour.ingest.ffreader.FastForwardReaderBase;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.BooleanFastForwardReader;
import org.apache.parquet.parqour.ingest.paging.DataPageMetadata;

/**
 * Created by sircodesalot on 6/24/15.
 */
public class PlainBooleanFastForwardReader extends FastForwardReaderBase implements BooleanFastForwardReader {
  public PlainBooleanFastForwardReader(DataPageMetadata metadata, ValuesType type) {
    super(metadata, type);

    this.currentEntryNumber = 0;
  }

  @Override
  public boolean readtf() {
    int byteNumber = (int)(this.currentEntryNumber / 8);
    int bitNumber = (int)(this.currentEntryNumber % 8);

    currentEntryNumber++;
    return (data[byteNumber] & (1 << bitNumber)) != 0;
  }

  @Override
  public void fastForwardTo(int entryNumber) {
    this.currentEntryNumber = entryNumber;
  }
}
