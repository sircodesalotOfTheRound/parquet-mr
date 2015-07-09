package org.apache.parquet.parqour.ingest.ffreader.segments;

import org.apache.parquet.parqour.exceptions.DataIngestException;

/**
 * Created by sircodesalot on 6/14/15.
 */
public abstract class PackedEncodingSegmentReader {

  protected final byte[] data;
  protected final int maximumValue;
  protected final int leftmostOneBitInMask;
  protected final int bitWidthForMaximumValue;
  protected final int readMask;
  protected final int sectionHeader;
  private final SegmentType type;

  protected int offset;

  protected PackedEncodingSegmentReader() {
    this.data = new byte[0];
    this.maximumValue = 0;
    this.leftmostOneBitInMask = 0;
    this.bitWidthForMaximumValue = 0;
    this.readMask = 0;
    this.sectionHeader = 0;
    this.type = SegmentType.ZERO;
  }

  protected PackedEncodingSegmentReader(SegmentType type, byte[] data, int startOffset, int maximumValue, int sectionHeader) {
    this.type = type;
    this.data = data;
    this.offset = startOffset;
    this.maximumValue = maximumValue;
    this.sectionHeader = sectionHeader;
    this.leftmostOneBitInMask = calculateLeftmostOne(maximumValue);
    this.bitWidthForMaximumValue = calculateBitWidthForMaximumValue(leftmostOneBitInMask);
    this.readMask = computeReadMask(leftmostOneBitInMask);
  }

  private int computeReadMask(int leftmostOneInMask) {
    // (x | x - 1) projects the rightmost bit all the way to the right.
    return leftmostOneInMask | (leftmostOneInMask - 1);
  }

  private int calculateLeftmostOne(int maximumValue) {
    int leftmostOne = maximumValue;

    // Use (x & x - 1) to remove the rightmost one -- until there are no ones left.
    // The second to last value is the one we actually want.
    for (int current = maximumValue & maximumValue - 1; current != 0; current = (current & current - 1)) {
      leftmostOne = current;
    }

    return leftmostOne;
  }

  private int calculateBitWidthForMaximumValue(int leftmostOne) {
    switch (leftmostOne) {
      case 0: return 0;
      case 1: return 1;
      case (1<<1): return 2;
      case (1<<2): return 3;
      case (1<<3): return 4;
      case (1<<4): return 5;
      case (1<<5): return 6;
      case (1<<6): return 7;
      case (1<<7): return 8;
      case (1<<8): return 9;
      case (1<<9): return 10;
      case (1<<10): return 11;
      case (1<<11): return 12;
      case (1<<12): return 13;
      case (1<<13): return 14;
      case (1<<14): return 15;
      case (1<<15): return 16;
      case (1<<16): return 17;
      case (1<<17): return 18;
      case (1<<18): return 19;
      case (1<<19): return 20;
      case (1<<20): return 21;
      case (1<<21): return 22;
      case (1<<22): return 23;
      case (1<<23): return 24;
      case (1<<24): return 25;
      case (1<<25): return 26;
      case (1<<26): return 27;
      case (1<<27): return 28;
      case (1<<28): return 29;
      case (1<<29): return 30;
      case (1<<30): return 31;
      default:
        throw new DataIngestException("Bit width too large");
    }
  }

  public static PackedEncodingSegmentReader createPackedEncodingSegmentReader(byte[] data, int offset, int maximumValue) {
    if (maximumValue == 0) {
      return ZeroValuePackedEncodingSegmentReader.INSTANCE;
    } else {
      int header = readHeader(data, offset);
      int headerLength = readHeaderLength(data, offset);
      int newOffset = offset + headerLength;

      if ((header & 1) == 0) {
        return new RLEEncodingSegmentReader(data, newOffset, maximumValue, header);
      } else {
        return new BitPackedSegmentReader(data, newOffset, maximumValue, header);
      }
    }
  }

  public static int readHeaderLength(byte[] data, int offset) {
    int length = 1;

    while ((data[++offset] & 0x80) != 0) {
      length++;
    }

    return length;
  }

  private static int readHeader(byte[] data, int offset) {
    // Todo: clean this up.
    int value = 0;
    int i = 0;
    int aByte;

    while (((aByte = data[++offset]) & 0x80) != 0) {
      value |= (aByte & 0x7F) << i;
      i += 7;
    }

    return value | (aByte << i);
  }

  public abstract boolean any();
  public abstract PackedEncodingSegmentReader generateReaderForNextSection();
  public abstract int readNext();
  public SegmentType type() { return this.type; }
}
