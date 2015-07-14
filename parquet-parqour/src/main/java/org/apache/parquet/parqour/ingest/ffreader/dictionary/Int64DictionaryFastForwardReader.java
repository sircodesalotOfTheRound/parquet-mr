package org.apache.parquet.parqour.ingest.ffreader.dictionary;

import org.apache.parquet.column.ValuesType;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.ingest.ffreader.FastForwardReaderBase;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.Int64FastForwardReader;
import org.apache.parquet.parqour.ingest.ffreader.segments.PackedEncodingSegmentReader;
import org.apache.parquet.parqour.ingest.paging.DataPageMetadata;

import java.io.IOException;

/**
 * Created by sircodesalot on 6/26/15.
 */
public class Int64DictionaryFastForwardReader extends FastForwardReaderBase implements Int64FastForwardReader {
  private final long[] dictionaryEntries;
  private PackedEncodingSegmentReader segment;

  public Int64DictionaryFastForwardReader(DataPageMetadata metadata, ValuesType type) {
    super(metadata, type);

    this.dataOffset = ++dataOffset;
    this.segment = PackedEncodingSegmentReader.createPackedEncodingSegmentReader(data, dataOffset, expandToBitWidth());
    this.dictionaryEntries = collectDictionaryEntries(metadata);
  }

  private long[] collectDictionaryEntries(DataPageMetadata metadata) {
    byte[] dictionaryData = collectDictionaryPageData(metadata.dictionaryPage());
    int entryCount = metadata.dictionaryPage().getDictionarySize();
    int dictionaryDataOffset = -1;

    long[] entries = new long[entryCount];

    for (int index = 0; index < entryCount; index++) {
      long value = ((long)dictionaryData[++dictionaryDataOffset] & 0xFF)
        | ((long)dictionaryData[++dictionaryDataOffset] & 0xFF) << 8
        | ((long)dictionaryData[++dictionaryDataOffset] & 0xFF) << 16
        | ((long)dictionaryData[++dictionaryDataOffset] & 0xFF) << 24
        | ((long)dictionaryData[++dictionaryDataOffset] & 0xFF) << 32
        | ((long)dictionaryData[++dictionaryDataOffset] & 0xFF) << 40
        | ((long)dictionaryData[++dictionaryDataOffset] & 0xFF) << 48
        | ((long)dictionaryData[++dictionaryDataOffset] & 0xFF) << 56;

      entries[index] = value;
    }

    return entries;
  }

  private byte[] collectDictionaryPageData(DictionaryPage dictionaryPage) {
    try {
      return dictionaryPage.getBytes().toByteArray();
    } catch (IOException ex) {
      throw new DataIngestException("Unable to load dictionary page.");
    }
  }

  private int expandToBitWidth() {
    int bitWidth = data[dataOffset];
    if (bitWidth == 0) {
      return 0;
    } else {
      return 1 << (bitWidth - 1);
    }
  }

  @Override
  public void fastForwardTo(int entryNumber) {

  }

  @Override
  public long readi64() {
    super.advanceRowNumber();
    if (!segment.any()) {
      this.segment = segment.generateReaderForNextSection();
    }

    int entryIndex = segment.readNext();
    return dictionaryEntries[entryIndex];
  }
}
