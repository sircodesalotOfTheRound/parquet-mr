package org.apache.parquet.parqour.ingest.ffreader.segments;

import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Created by sircodesalot on 6/23/15.
 */
public class DeltaPackedSegmentReader extends PackedEncodingSegmentReader {
  private final int lastBufferItemIndex;

  private final int blockSizeInValues;
  private final int miniBlocksInABlock;
  private final int miniBlockSize;

  private final int totalValues;
  private int totalRead;
  private int valuesBuffered;

  private int minimumDeltaInBlock;
  private long previousValue;

  private final int[] bitWidthsForBlocks;
  private final int bufferLength;
  private final int[] buffer;
  private int bufferOffset;

  public DeltaPackedSegmentReader(byte[] data, int offset) {
    super(SegmentType.DELTA_PACKED, data, offset, 0, 0);

    this.blockSizeInValues = readVariableLengthInteger();
    this.miniBlocksInABlock = readVariableLengthInteger();
    this.miniBlockSize = readMiniBlockSize(blockSizeInValues, miniBlocksInABlock);
    this.totalValues = readVariableLengthInteger();

    this.bufferLength = miniBlocksInABlock * miniBlockSize;
    this.buffer = new int[bufferLength];
    this.bitWidthsForBlocks = new int[miniBlocksInABlock];

    this.lastBufferItemIndex = buffer.length - 1;
    this.bufferOffset = lastBufferItemIndex;
    this.minimumDeltaInBlock = 0;
    this.totalRead = 0;

    // Buffer the first item from the header.
    this.buffer[lastBufferItemIndex] = readZigZagVariableLengthInteger();
    this.valuesBuffered = 1;
  }

  public DeltaPackedSegmentReader(byte[] data) {
    this(data, -1);
  }


  private int readVariableLengthInteger() {
    int value = 0;
    int i = 0;
    int aByte;

    while (((aByte = data[++offset]) & 0x80) != 0) {
      value |= (aByte & 0x7F) << i;
      i += 7;
    }

    return value | (aByte << i);
  }

  public int readZigZagVariableLengthInteger() {
    int raw = readVariableLengthInteger();
    int temp = (((raw << 31) >> 31) ^ raw) >> 1;
    return temp ^ (raw & (1 << 31));
  }

  private void loadBuffer() {
    this.minimumDeltaInBlock = readMimimumDeltaInCurrentBlock();

    for (int index = 0; index < miniBlocksInABlock; index++) {
      bitWidthsForBlocks[index] = data[++offset];
    }

    int bufferWriteOffset = 0;
    for (int miniBlockIndex = 0; miniBlockIndex < miniBlocksInABlock; miniBlockIndex++) {
      for (int index = 0; index < miniBlockSize && valuesBuffered < totalValues; index += 8) {
        int bitWidthForBlock = bitWidthsForBlocks[miniBlockIndex];
        // Bit packer doesn't work for zero length regions. We have to manually zero in that case.
        if (bitWidthForBlock != 0) {
          BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidthsForBlocks[miniBlockIndex]);
          packer.unpack8Values(data, ++offset, buffer, bufferWriteOffset);
          bufferWriteOffset += 8;

          offset += packer.getBitWidth() - 1;

        } else {
          buffer[bufferWriteOffset++] = 0;
          buffer[bufferWriteOffset++] = 0;
          buffer[bufferWriteOffset++] = 0;
          buffer[bufferWriteOffset++] = 0;
          buffer[bufferWriteOffset++] = 0;
          buffer[bufferWriteOffset++] = 0;
          buffer[bufferWriteOffset++] = 0;
          buffer[bufferWriteOffset++] = 0;
        }

        valuesBuffered += 8;
      }
    }
  }

  private int readMiniBlockSize(int blockSizeInValues, int minimumBlockNumInABlock) {
    return (int) ((double) blockSizeInValues / (double) minimumBlockNumInABlock);
  }

  private int readMimimumDeltaInCurrentBlock() {
    return readZigZagVariableLengthInteger();
  }

  private long nextValue() {
    long delta = buffer[bufferOffset] + previousValue + minimumDeltaInBlock;
    previousValue = delta;
    totalRead += 1;
    bufferOffset += 1;

    // Update the buffer if neccesary.
    if (bufferOffset >= bufferLength
      && valuesBuffered < totalValues) {
      loadBuffer();
      bufferOffset = 0;
    }

    return delta;
  }

  @Deprecated
  public int readNext() {
    throw new NotImplementedException();
  }

  public int readi32() {
    return (int) nextValue();
  }

  public long readi64() {
    return nextValue();
  }

  @Override
  public boolean any() {
    return totalRead < totalValues;
  }

  public int determineEndingOffset() {
    int[] pseudoBitWidthsForMiniBlocks = new int[bitWidthsForBlocks.length];
    int pseudoValuesBuffered = valuesBuffered;
    int offsetSavePoint = offset;

    while (pseudoValuesBuffered < totalValues) {
      int unusedMinimumDeltaInBlock = readMimimumDeltaInCurrentBlock();

      for (int index = 0; index < miniBlocksInABlock; index++) {
        pseudoBitWidthsForMiniBlocks[index] = data[++offset];
      }

      for (int miniBlockIndex = 0; miniBlockIndex < miniBlocksInABlock && pseudoValuesBuffered < totalValues; miniBlockIndex++) {
        for (int index = 0; index < miniBlockSize; index += 8) {
          offset += pseudoBitWidthsForMiniBlocks[miniBlockIndex];
          pseudoValuesBuffered += 8;
        }
      }
    }

    // Reset the old offset point, and then return.
    int resultOffset = this.offset;
    this.offset = offsetSavePoint;
    return resultOffset;
  }

  @Override
  public PackedEncodingSegmentReader generateReaderForNextSection() {
    throw new NotImplementedException();
  }
}
