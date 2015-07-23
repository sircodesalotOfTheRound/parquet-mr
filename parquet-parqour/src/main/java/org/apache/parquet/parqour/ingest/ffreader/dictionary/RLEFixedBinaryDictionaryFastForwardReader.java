package org.apache.parquet.parqour.ingest.ffreader.dictionary;

import org.apache.parquet.column.ValuesType;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.ingest.ffreader.FastForwardReaderBase;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.BinaryFastForwardReader;
import org.apache.parquet.parqour.ingest.ffreader.segments.PackedEncodingSegmentReader;
import org.apache.parquet.parqour.ingest.paging.DataPageMetadata;

import java.io.IOException;

/**
 * Created by sircodesalot on 6/13/15.
 */
public final class RLEFixedBinaryDictionaryFastForwardReader extends FastForwardReaderBase
  implements BinaryFastForwardReader
{
  private PackedEncodingSegmentReader segment;
  private final String[] dictionaryEntriesAsStrings;
  private final byte[][] dictionaryEntries;

  public RLEFixedBinaryDictionaryFastForwardReader(DataPageMetadata metadata, ValuesType type) {
    super(metadata, type);

    this.dictionaryEntries = this.readDictionaryEntries(metadata);
    this.dictionaryEntriesAsStrings = new String[metadata.dictionaryPage().getDictionarySize()];
    this.segment = PackedEncodingSegmentReader.createPackedEncodingSegmentReader(data, 0, expandToBitWidth());
  }

  private byte[][] readDictionaryEntries(DataPageMetadata metadata) {
    DictionaryPage dictionaryPage = metadata.dictionaryPage();
    int dictionarySize = dictionaryPage.getDictionarySize();
    int typeLength = metadata.typeLength();
    byte[] dictionaryData = collectDictionaryPageData(dictionaryPage);

    int dictionaryOffset = 0;
    byte[][] dictionaryEntries = new byte[dictionarySize][typeLength];
    for (int entry = 0; entry < dictionarySize; entry++) {
      for (int index = 0; index < typeLength; index++) {
        dictionaryEntries[entry][index] = dictionaryData[dictionaryOffset++];
      }
    }

    return dictionaryEntries;
  }

  private byte[] collectDictionaryPageData(DictionaryPage dictionaryPage) {
    try {
      return dictionaryPage.getBytes().toByteArray();
    } catch (IOException ex) {
      throw new DataIngestException("Unable to load dictionary page.");
    }
  }

  private int expandToBitWidth() {
    int bitWidth = data[0];
    if (bitWidth == 0) {
      return 0;
    } else {
      return 1 << (bitWidth - 1);
    }
  }

  public int readNextDictionaryEntryIndex() {
    super.advanceEntryNumber();
    if (!segment.any()) {
      this.segment = segment.generateReaderForNextSection();
    }

    return segment.readNext();
  }

  public byte[] getDictionaryEntryIndexAsBytes(int index) {
    return dictionaryEntries[index];
  }


  @Override
  public void fastForwardTo(int entryNumber) {

  }

  @Override
  public String readString() {
    super.advanceEntryNumber();
    if (!segment.any()) {
      this.segment = segment.generateReaderForNextSection();
    }

    int entryIndex = segment.readNext();
    // If the entry doesn't already exist, set and return. Else just return.
    if (dictionaryEntriesAsStrings[entryIndex] == null) {
      return dictionaryEntriesAsStrings[entryIndex] = new String(dictionaryEntries[entryIndex]);
    } else {
      return dictionaryEntriesAsStrings[entryIndex];
    }
  }

  @Override
  public byte[] readBytes() {
    return new byte[0];
  }
}

