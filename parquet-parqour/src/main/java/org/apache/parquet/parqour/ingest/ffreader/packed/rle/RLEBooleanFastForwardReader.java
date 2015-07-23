package org.apache.parquet.parqour.ingest.ffreader.packed.rle;

import org.apache.parquet.column.ValuesType;
import org.apache.parquet.parqour.ingest.ffreader.FastForwardReaderBase;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.BooleanFastForwardReader;
import org.apache.parquet.parqour.ingest.ffreader.segments.PackedEncodingSegmentReader;
import org.apache.parquet.parqour.ingest.paging.DataPageMetadata;

/**
 * Created by sircodesalot on 6/13/15.
 */
public final class RLEBooleanFastForwardReader extends FastForwardReaderBase implements BooleanFastForwardReader {
  private static int SIZEOF_BOOLEAN = 1;
  private static int TRUE = 1;
  private static final int SECTION_LENGTH_INT32_SIZE = 4;

  private PackedEncodingSegmentReader segment;

  public RLEBooleanFastForwardReader(DataPageMetadata metadata, ValuesType type) {
    super(metadata, type);

    this.segment = PackedEncodingSegmentReader
      .createPackedEncodingSegmentReader(data, dataOffset + SECTION_LENGTH_INT32_SIZE, SIZEOF_BOOLEAN);
  }

  @Override
  public boolean readtf() {
    if (!segment.any()) {
      this.segment = segment.generateReaderForNextSection();
    }

    super.advanceEntryNumber();
    int value = segment.readNext();
    return value == TRUE;
  }

  @Override
  public void fastForwardTo(int entryNumber) {
    switch (segment.type()) {
      case ZERO: /* No-op*/
        break;

      default:
        while (currentEntryNumber < entryNumber) {
          this.readtf();
        }
    }

    currentEntryNumber = entryNumber;
  }
}

