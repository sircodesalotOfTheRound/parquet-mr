package org.apache.parquet.parqour.ingest.ffreader.delta;

import org.apache.parquet.column.ValuesType;
import org.apache.parquet.parqour.ingest.disk.pages.info.DataPageInfo;
import org.apache.parquet.parqour.ingest.ffreader.FastForwardReaderBase;
import org.apache.parquet.parqour.ingest.ffreader.binary.BinaryTrie;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.BinaryFastForwardReader;
import org.apache.parquet.parqour.ingest.ffreader.segments.DeltaPackedSegmentReader;
import org.apache.parquet.parqour.ingest.paging.DataPageMetadata;

/**
 * Created by sircodesalot on 6/25/15.
 */
public class DeltaByteArrayBinaryFastForwardReader extends FastForwardReaderBase implements BinaryFastForwardReader {
  private final BinaryTrie trie;
  private final DeltaPackedSegmentReader prefixReader;
  private final DeltaPackedSegmentReader suffixReader;

  public DeltaByteArrayBinaryFastForwardReader(DataPageMetadata metadata, ValuesType type) {
    super(metadata, type);

    this.prefixReader = new DeltaPackedSegmentReader(data);
    this.suffixReader = generateSuffixReader(null, prefixReader);
    this.dataOffset = suffixReader.determineEndingOffset() + 1;
    this.trie = new BinaryTrie(45, 5000);
  }

  public DeltaByteArrayBinaryFastForwardReader(DataPageInfo info, ValuesType values) {
    super(info, values);

    this.prefixReader = new DeltaPackedSegmentReader(data, info.contentOffset() - 1);
    this.suffixReader = generateSuffixReader(info, prefixReader);
    this.dataOffset = suffixReader.determineEndingOffset() + 1;
    this.trie = new BinaryTrie(45, 5000);
  }

  public DeltaPackedSegmentReader generateSuffixReader(DataPageInfo info, DeltaPackedSegmentReader prefixReader) {
    int suffixOffset = prefixReader.determineEndingOffset();
    return new DeltaPackedSegmentReader(data, suffixOffset);
  }

  @Deprecated
  public DeltaPackedSegmentReader generateSuffixReader(DeltaPackedSegmentReader prefixReader) {
    int suffixOffset = prefixReader.determineEndingOffset();
    return new DeltaPackedSegmentReader(data, suffixOffset);
  }

  @Override
  public String readString() {
    super.advanceEntryNumber();
    int prefixLength = prefixReader.readi32();
    int length = prefixLength + suffixReader.readi32();

    String result = new String(data, dataOffset, length);
    //String result = trie.getString(data, dataOffset, length);
    dataOffset += length;

    return result;
  }

  @Override
  public byte[] readBytes() {
    super.advanceEntryNumber();
    int prefixLength = prefixReader.readi32();
    int length = prefixLength + suffixReader.readi32();

    byte[] result = trie.getByteArray(data, dataOffset, length);
    dataOffset += length;

    return result;
  }

  @Override
  public void fastForwardTo(int entryNumber) {

  }

}
