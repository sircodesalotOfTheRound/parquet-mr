package org.apache.parquet.parqour.ingest.ffreader.segments;

import org.apache.parquet.parqour.exceptions.DataIngestException;

import static org.apache.parquet.parqour.ingest.ffreader.segments.SegmentType.BIT_PACKED;

/**
 * Created by sircodesalot on 6/14/15.
 */
public final class BitPackedSegmentReader extends PackedEncodingSegmentReader {
  // For some reason the number of items packed should be multiplied by eight.
  private static final int BIT_PACKED_COUNT_MULTIPLIER = 8;
  private static final int STANDARD_BYTE_WIDTH = 8;

  private int bytesLeftToRead;        // Number of bytes left to read.
  private int packedValuesRemaining;  // Number of items left to read.

  private long buffer;        // 64-bit buffer.
  private int remainingBufferLength;

  public BitPackedSegmentReader(byte[] data, int startOffset, int maximumValue, int sectionHeader) {
    super(BIT_PACKED, data, startOffset, maximumValue, sectionHeader);
    // Read the length of the section.
    this.packedValuesRemaining = computeNumberOfItemsInSection();
    this.bytesLeftToRead = computeBytesLeftToRead(packedValuesRemaining, bitWidthForMaximumValue);

    this.buffer = loadBuffer();
    this.remainingBufferLength = 64;
  }

  private int computeNumberOfItemsInSection() {
    int numberOfPackedItems = super.sectionHeader >>> 1;
    return numberOfPackedItems * BIT_PACKED_COUNT_MULTIPLIER;
  }

  private int computeBytesLeftToRead(int totalItemsInSection, int bitWidthForMaximumValue) {
    return (int)Math.ceil((totalItemsInSection * bitWidthForMaximumValue) / (double)STANDARD_BYTE_WIDTH);
  }

  private long loadBuffer() {
    long buffer = 0;

    buffer |= ((bytesLeftToRead-- > 0) ? ((long)data[++offset] & 0xFF) : 0L);
    buffer |= ((bytesLeftToRead-- > 0) ? (((long)data[++offset] & 0xFF) << 8) : 0L);
    buffer |= ((bytesLeftToRead-- > 0) ? (((long)data[++offset] & 0xFF) << 16) : 0L);
    buffer |= ((bytesLeftToRead-- > 0) ? (((long)data[++offset] & 0xFF) << 24) : 0L);
    buffer |= ((bytesLeftToRead-- > 0) ? (((long)data[++offset] & 0xFF)  << 32) : 0L);
    buffer |= ((bytesLeftToRead-- > 0) ? (((long)data[++offset] & 0xFF) << 40) : 0L);
    buffer |= ((bytesLeftToRead-- > 0) ? (((long)data[++offset] & 0xFF) << 48) : 0L);
    buffer |= ((bytesLeftToRead-- > 0) ? (((long)data[++offset] & 0xFF) << 56) : 0L);

    return buffer;
  }

  public int readNext() {
    if (packedValuesRemaining <= 0) {
      throw new DataIngestException("No more data to ingest");
    }

    // Three cases to test for:
    // (1) We have enough data on the buffer to do a normal read.
    // (2) We don't have enough data, but at least we stopped right on the boundary.
    // (3) We don't have enough data, and we're at a weird boundary.
    if (bitWidthForMaximumValue <= remainingBufferLength) {
      return normalRead();

    } else if (remainingBufferLength == 0) {
      this.buffer = loadBuffer();
      this.remainingBufferLength = 64;
      return normalRead();

    } else {
      return jaggedRead();
    }
  }

  private int normalRead() {
    // The result is the last few bits on the buffer.
    // Mask those off, then shift the buffer to the right 'width' number of bits.
    int result = (int) (buffer & readMask);
    this.buffer >>>= bitWidthForMaximumValue;

    // Recalculate offsets.
    this.remainingBufferLength -= bitWidthForMaximumValue;
    this.packedValuesRemaining--;

    return result;
  }

  private int jaggedRead() {
    // (1) Capture the old bufffer.
    long oldBuffer = this.buffer;

    // (2) Apply the remaining bits in the buffer.
    int result = (int)(oldBuffer & readMask);

    // (3) Recompute the remaining bits left to read.
    int bitsLeftToRead = (bitWidthForMaximumValue - remainingBufferLength);

    // (4) Load the new buffer
    this.buffer = loadBuffer();
    this.remainingBufferLength = 64;

    // (5) Create an offsetted read mask to read just the beginning of the next buffer.
    // then apply the read mask, and shift the value to fit onto the left of the already
    // read portion.
    int offsettedReadMask = (readMask >>> (bitWidthForMaximumValue - bitsLeftToRead));
    int resultAfterMaskApplied = (int)(this.buffer & offsettedReadMask);
    result |= resultAfterMaskApplied << (bitWidthForMaximumValue - bitsLeftToRead);

    // (6) Recompute offsets.
    this.buffer >>>= bitsLeftToRead;
    this.remainingBufferLength -= bitsLeftToRead;
    this.packedValuesRemaining--;

    return result;
  }

  public boolean any() { return this.packedValuesRemaining > 0; }

  public PackedEncodingSegmentReader generateReaderForNextSection() {
    return PackedEncodingSegmentReader.createPackedEncodingSegmentReader(data, offset, maximumValue);
  }
}
