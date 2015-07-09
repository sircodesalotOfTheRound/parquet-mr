package org.apache.parquet.parqour.ingest.ffreader.segments;

/**
 * Created by sircodesalot on 6/14/15.
 */
public class RLEEncodingSegmentReader extends PackedEncodingSegmentReader {
  private final int currentValue;
  private int remainingNumberOfEntries;

  public RLEEncodingSegmentReader(byte[] data, int startOffset, int maximumValue, int sectionHeader) {
    super(SegmentType.RUN_LENGTH_ENCODING, data, startOffset, maximumValue, sectionHeader);

    this.remainingNumberOfEntries = computeRemainingNumberOfEntries();
    this.currentValue = computeCurrentValue();
  }

  private int computeRemainingNumberOfEntries() {
    return super.sectionHeader >>> 1;
  }

  private int computeCurrentValue() {
    // We add one so that we can use prefix-decrement.
    int numberOfBytesToRead = computeNumberOfBytesRequiredToReadThisBitWidth() + 1;

    int currentValue = 0;
    currentValue |= ((--numberOfBytesToRead > 0) ? (((int)data[++offset] & 0xFF)) : 0L);
    currentValue |= ((--numberOfBytesToRead > 0) ? (((int)data[++offset] & 0xFF) << 8) : 0L);
    currentValue |= ((--numberOfBytesToRead > 0) ? (((int)data[++offset] & 0xFF) << 16) : 0L);
    currentValue |= ((--numberOfBytesToRead > 0) ? (((int)data[++offset] & 0xFF) << 24) : 0L);

    return currentValue;
  }

  private int computeNumberOfBytesRequiredToReadThisBitWidth() {
    return (super.bitWidthForMaximumValue + 7) / 8;
  }

  @Override
  public boolean any() {
    return remainingNumberOfEntries > 0;
  }

  @Override
  public PackedEncodingSegmentReader generateReaderForNextSection() {
    return PackedEncodingSegmentReader.createPackedEncodingSegmentReader(data, offset, maximumValue);
  }

  @Override
  public int readNext() {
    remainingNumberOfEntries--;
    return currentValue;
  }
}
