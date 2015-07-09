package org.apache.parquet.parqour.ingest.ffreader.plain;

import org.apache.parquet.parqour.ingest.ffreader.FastForwardReaderBase;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.BooleanFastForwardReader;
import org.apache.parquet.parqour.ingest.paging.DataPageMetadata;
import org.apache.parquet.column.ValuesType;

/**
 * Created by sircodesalot on 6/24/15.
 */
public class PlainBooleanFastForwardReader extends FastForwardReaderBase implements BooleanFastForwardReader {
  public PlainBooleanFastForwardReader(DataPageMetadata metadata, ValuesType type) {
    super(metadata, type);

    this.currentRowOnPage = 0;
  }

  @Override
  public boolean readtf() {
    int byteNumber = (int)(this.currentRowOnPage / 8);
    int bitNumber = (int)(this.currentRowOnPage % 8);

    currentRow++;
    currentRowOnPage++;
    return (data[byteNumber] & (1 << bitNumber)) != 0;
  }

  @Override
  public void fastForwardTo(int rowNumber) {
    long jumpDistance = (rowNumber - currentRow);

    this.currentRow = rowNumber;
    this.currentRowOnPage += jumpDistance;
  }
}
