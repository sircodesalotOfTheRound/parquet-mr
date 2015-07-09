package org.apache.parquet.parqour.ingest.ffreader.delta;

import org.apache.parquet.parqour.ingest.ffreader.FastForwardReaderBase;
import org.apache.parquet.parqour.ingest.ffreader.binary.BinaryTrie;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.BinaryFastForwardReader;
import org.apache.parquet.parqour.ingest.ffreader.segments.DeltaPackedSegmentReader;
import org.apache.parquet.parqour.ingest.paging.DataPageMetadata;
import org.apache.parquet.column.ValuesType;

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
    this.suffixReader = generateSuffixReader(prefixReader);
    this.dataOffset = suffixReader.determineEndingOffset() + 1;
    this.trie = new BinaryTrie(45, 5000);
  }

  public DeltaPackedSegmentReader generateSuffixReader(DeltaPackedSegmentReader prefixReader) {
    int suffixOffset = prefixReader.determineEndingOffset();
    return new DeltaPackedSegmentReader(data, suffixOffset);
  }

  @Override
  public String readString() {
    int prefixLength = prefixReader.readi32();
    int length = prefixLength + suffixReader.readi32();

    String result = trie.getString(data, dataOffset, length);
    dataOffset += length;

    return result;
  }

  @Override
  public byte[] readBytes() {
    int prefixLength = prefixReader.readi32();
    int length = prefixLength + suffixReader.readi32();

    byte[] result = trie.getByteArray(data, dataOffset, length);
    dataOffset += length;

    return result;
  }

  @Override
  public void fastForwardTo(int rowNumber) {

  }
}
