package org.apache.parquet.parqour.ingest.ffreader.dictionary;

import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.ingest.ffreader.FastForwardReaderBase;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.Int32FastForwardReader;
import org.apache.parquet.parqour.ingest.ffreader.segments.PackedEncodingSegmentReader;
import org.apache.parquet.parqour.ingest.paging.DataPageMetadata;
import org.apache.parquet.column.ValuesType;
import org.apache.parquet.column.page.DictionaryPage;

import java.io.IOException;

/**
 * Created by sircodesalot on 6/26/15.
 */
public class Int32DictionaryFastForwardReader extends FastForwardReaderBase implements Int32FastForwardReader {
  private final int[] dictionaryEntries;
  private PackedEncodingSegmentReader segment;

  public Int32DictionaryFastForwardReader(DataPageMetadata metadata, ValuesType type) {
    super(metadata, type);

    this.dataOffset = ++dataOffset;
    this.segment = PackedEncodingSegmentReader.createPackedEncodingSegmentReader(data, dataOffset, expandToBitWidth());
    this.dictionaryEntries = collectDictionaryEntries(metadata);
  }

  private int[] collectDictionaryEntries(DataPageMetadata metadata) {
    byte[] dictionaryData = collectDictionaryPageData(metadata.dictionaryPage());
    int entryCount = metadata.dictionaryPage().getDictionarySize();
    int[] entries = new int[entryCount];
    int dictionaryDataOffset = -1;

    for (int index = 0; index < entryCount; index++) {
      int value = (dictionaryData[++dictionaryDataOffset] & 0xFF)
        | (dictionaryData[++dictionaryDataOffset] & 0xFF) << 8
        | (dictionaryData[++dictionaryDataOffset] & 0xFF) << 16
        | (dictionaryData[++dictionaryDataOffset] & 0xFF) << 24;

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
  public int readi32() {
    super.advanceRowNumber();
    if (!segment.any()) {
      this.segment = segment.generateReaderForNextSection();
    }

    int entryIndex = segment.readNext();
    return dictionaryEntries[entryIndex];
  }

  @Override
  public void fastForwardTo(int rowNumber) {

  }
}
