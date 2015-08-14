package org.apache.parquet.parqour.ingest.ffreader.plain;

import org.apache.parquet.column.ValuesType;
import org.apache.parquet.parqour.ingest.disk.pages.info.DataPageInfo;
import org.apache.parquet.parqour.ingest.ffreader.FastForwardReaderBase;
import org.apache.parquet.parqour.ingest.ffreader.binary.BinaryTrie;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.BinaryFastForwardReader;
import org.apache.parquet.parqour.ingest.paging.DataPageMetadata;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Created by sircodesalot on 6/13/15.
 */
public final class PlainFixedBinaryFastForwardReader extends FastForwardReaderBase
  implements BinaryFastForwardReader {

  // Move trie to column.
  private final BinaryTrie trie = new BinaryTrie(45, 5000);
  private final int fieldLength;

  public PlainFixedBinaryFastForwardReader(DataPageMetadata metadata, ValuesType type) {
    super(metadata, type);
    this.fieldLength = super.metadata.typeLength();
  }

  public PlainFixedBinaryFastForwardReader(DataPageInfo info, ValuesType values) {
    super(info, values);

    this.fieldLength = info.typeLength();
  }

  // TODO: Remove deprecated code.

  @Deprecated
  public int readByteArrayOffset() {
    throw new NotImplementedException();
  }

  public String readString() {
    super.advanceEntryNumber();

    int startingOffset = ++dataOffset;
    dataOffset += fieldLength - 1;
    // Todo: remove tries.
    return trie.getString(data, startingOffset, fieldLength);
  }

  @Override
  public byte[] readBytes() {
    super.advanceEntryNumber();

    int startingOffset = ++dataOffset;
    dataOffset += fieldLength - 1;
    return trie.getByteArray(data, startingOffset, fieldLength);
  }

  @Deprecated
  public byte[] data() { throw new NotImplementedException(); }

  @Override
  public void fastForwardTo(int entryNumber) {
    long jumpDistance = (entryNumber - currentEntryNumber) * fieldLength;

    dataOffset += jumpDistance;
    currentEntryNumber = entryNumber;
  }
}

