package org.apache.parquet.parqour.ingest.ffreader.plain;

import org.apache.parquet.column.ValuesType;
import org.apache.parquet.parqour.ingest.disk.pages.info.DataPageInfo;
import org.apache.parquet.parqour.ingest.ffreader.FastForwardReaderBase;
import org.apache.parquet.parqour.ingest.ffreader.binary.BinaryTrie;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.BinaryFastForwardReader;
import org.apache.parquet.parqour.ingest.paging.DataPageMetadata;

import java.util.Arrays;

/**
 * Created by sircodesalot on 6/13/15.
 */
public final class PlainBinaryFastForwardReader extends FastForwardReaderBase
  implements BinaryFastForwardReader
{
  private final BinaryTrie trie = new BinaryTrie(45, 5000);
  private static final int SIZEOF_INT = 4;

  @Deprecated
  public PlainBinaryFastForwardReader(DataPageMetadata metadata, ValuesType type) {
    super(metadata, type);
  }

  public PlainBinaryFastForwardReader(DataPageInfo info, ValuesType values) {
    super(info, values);
  }

  @Deprecated
  public byte[] generateByteArrayFromByteArrayOffset(int offset) {
    int length = readInt32WithExplicitOffset(offset);
    int start = offset + SIZEOF_INT;
    int end = start + length;

    return Arrays.copyOfRange(data, start, end);
  }

  private int readInt32AndImplictlyAdvanceOffset() {
    return (data[dataOffset++] & 0xFF)
      | (data[dataOffset++] & 0xFF) << 8
      | (data[dataOffset++] & 0xFF) << 16
      | (data[dataOffset] & 0xFF) << 24;
  }

  private int readInt32WithExplicitOffset(int offset) {
    return (data[offset++] & 0xFF)
      | (data[offset++] & 0xFF) << 8
      | (data[offset++] & 0xFF) << 16
      | (data[offset] & 0xFF) << 24;
  }

  public byte[] data() { return super.data; }

  @Override
  public void fastForwardTo(int entryNumber) {
    for (int index = (int)currentEntryNumber; index < entryNumber; index++) {
      dataOffset += 1;
      int length = this.readInt32AndImplictlyAdvanceOffset();
      dataOffset += length;
    }

    super.currentEntryNumber = entryNumber;
  }

  @Override
  public String readString() {
    super.advanceEntryNumber();

    int startingOffset = ++dataOffset;
    int length = this.readInt32AndImplictlyAdvanceOffset();
    dataOffset += length;

    // TODO: Don't do this.
    return trie.getString(data, startingOffset + SIZEOF_INT, length);
  }

  @Override
  public byte[] readBytes() {
    super.advanceEntryNumber();

    int startingOffset = ++dataOffset;
    int length = this.readInt32AndImplictlyAdvanceOffset();
    dataOffset += length;

    // TODO: Don't do this.
    return trie.getByteArray(data, startingOffset + SIZEOF_INT, length);
  }
}

