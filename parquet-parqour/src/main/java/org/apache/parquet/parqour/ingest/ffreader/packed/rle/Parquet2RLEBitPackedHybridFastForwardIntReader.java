package org.apache.parquet.parqour.ingest.ffreader.packed.rle;

import org.apache.parquet.column.ValuesType;
import org.apache.parquet.parqour.ingest.disk.pages.info.DataPageInfo;
import org.apache.parquet.parqour.ingest.ffreader.FastForwardReaderBase;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.RelationshipLevelFastForwardReader;
import org.apache.parquet.parqour.ingest.ffreader.segments.PackedEncodingSegmentReader;
import org.apache.parquet.parqour.ingest.paging.DataPageMetadata;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

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

  public Parquet2RLEBitPackedHybridFastForwardIntReader(DataPageInfo info, ValuesType type) {
    super(info, type);

    this.maximumValue = info.relationshipLevelForType(type);
    this.channelData = info.dataForType(type);
    this.dataOffset = info.computeOffset(type) - 1;

    this.segment = PackedEncodingSegmentReader
      .createPackedEncodingSegmentReader(channelData, dataOffset, maximumValue);
  }

  @Override
  public int nextRelationshipLevel() {
    if (!segment.any()) {
      this.segment = segment.generateReaderForNextSection();
    }

    super.advanceEntryNumber();
    return segment.readNext();
  }

  @Override
  public void fastForwardTo(int entryNumber) {
    switch (segment.type()) {
      case ZERO: /* No-op*/
        break;

      default:
        while (currentEntryNumber < entryNumber) {
          this.nextRelationshipLevel();
        }
    }

    currentEntryNumber = entryNumber;
  }
}

