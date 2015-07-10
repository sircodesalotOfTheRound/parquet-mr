package org.apache.parquet.parqour.ingest.ffreader.packed.rle;

import org.apache.parquet.parqour.ingest.ffreader.FastForwardReaderBase;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.RelationshipLevelFastForwardReader;
import org.apache.parquet.parqour.ingest.ffreader.segments.PackedEncodingSegmentReader;
import org.apache.parquet.parqour.ingest.paging.DataPageMetadata;
import org.apache.parquet.column.ValuesType;

/**
 * Created by sircodesalot on 6/13/15.
 */
public final class Parquet2RLEBitPackedHybridFastForwardIntReader extends FastForwardReaderBase implements RelationshipLevelFastForwardReader {
  private byte[] channelData;
  private PackedEncodingSegmentReader segment;
  private final int maximumValue;

  public Parquet2RLEBitPackedHybridFastForwardIntReader(DataPageMetadata metadata, ValuesType type) {
    super(metadata, type);

    this.maximumValue = metadata.computeMaximumValueForValueType(type);
    this.channelData = metadata.getParquet2RLOrDLChannelData(type);

    this.segment = PackedEncodingSegmentReader
      .createPackedEncodingSegmentReader(channelData, -1, maximumValue);
  }

  @Override
  public int nextRelationshipLevel() {
    if (!segment.any()) {
      this.segment = segment.generateReaderForNextSection();
    }

    super.advanceRowNumber();
    return segment.readNext();
  }

  @Override
  public void fastForwardTo(int entryNumber) {
    switch (segment.type()) {
      case ZERO: /* No-op*/
        break;

      default:
        while (currentRow < entryNumber) {
          this.nextRelationshipLevel();
        }
    }

    currentRow = entryNumber;
  }
}

