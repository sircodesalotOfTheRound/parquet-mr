package org.apache.parquet.parqour.ingest.ffreader.dictionary;

import org.apache.parquet.column.ValuesType;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.ingest.disk.pages.info.DataPageInfo;
import org.apache.parquet.parqour.ingest.disk.pages.info.DictionaryPageInfo;
import org.apache.parquet.parqour.ingest.ffreader.FastForwardReaderBase;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.BinaryFastForwardReader;
import org.apache.parquet.parqour.ingest.ffreader.segments.PackedEncodingSegmentReader;
import org.apache.parquet.parqour.ingest.paging.DataPageMetadata;

import java.io.IOException;

/**
 * Created by sircodesalot on 6/13/15.
 */
public final class RLEBinaryDictionaryFastForwardReader extends FastForwardReaderBase
  implements BinaryFastForwardReader
{
  private PackedEncodingSegmentReader segment;
  private final String[] dictionaryEntriesAsStrings;
  private final byte[][] dictionaryEntries;

  public RLEBinaryDictionaryFastForwardReader(DataPageMetadata metadata, ValuesType type) {
    super(metadata, type);

    this.dictionaryEntries = this.readDictionaryEntries(metadata);
    this.dictionaryEntriesAsStrings = new String[metadata.dictionaryPage().getDictionarySize()];
    this.segment = PackedEncodingSegmentReader.createPackedEncodingSegmentReader(data, 0, expandToBitWidth());
  }

  public RLEBinaryDictionaryFastForwardReader(DataPageInfo info, ValuesType values) {
    super(info, values);

    this.dictionaryEntries = this.readDictionaryEntries(info);
    this.dictionaryEntriesAsStrings = new String[(int) info.dictionaryPage().entryCount()];
    this.segment = PackedEncodingSegmentReader
      .createPackedEncodingSegmentReader(data, info.contentOffset(), expandToBitWidth(info));
  }


  private byte[][] readDictionaryEntries(DataPageInfo info) {
    DictionaryPageInfo dictionaryPage = info.dictionaryPage();
    int dictionarySize = (int)dictionaryPage.entryCount();
    byte[] dictionaryData = dictionaryPage.data();

    int dictionaryOffset = dictionaryPage.startingOffset();
    byte[][] dictionaryEntries = new byte[dictionarySize][];
    for (int entry = 0; entry < dictionarySize; entry++) {

      int entryLength = (dictionaryData[dictionaryOffset++] & 0xFF)
        | (dictionaryData[dictionaryOffset++] & 0xFF) << 8
        | (dictionaryData[dictionaryOffset++] & 0xFF) << 16
        | (dictionaryData[dictionaryOffset++] & 0xFF) << 24;

      dictionaryEntries[entry] = new byte[entryLength];
      for (int index = 0; index < entryLength; index++) {
        dictionaryEntries[entry][index] = dictionaryData[dictionaryOffset++];
      }
    }

    return dictionaryEntries;
  }

  @Deprecated
  private byte[][] readDictionaryEntries(DataPageMetadata metadata) {
    DictionaryPage dictionaryPage = metadata.dictionaryPage();
    int dictionarySize = dictionaryPage.getDictionarySize();
    byte[] dictionaryData = collectDictionaryPageData(dictionaryPage);

    int dictionaryOffset = 0;
    byte[][] dictionaryEntries = new byte[dictionarySize][];
    for (int entry = 0; entry < dictionarySize; entry++) {

      int entryLength = (dictionaryData[dictionaryOffset++] & 0xFF)
        | (dictionaryData[dictionaryOffset++] & 0xFF) << 8
        | (dictionaryData[dictionaryOffset++] & 0xFF) << 16
        | (dictionaryData[dictionaryOffset++] & 0xFF) << 24;

      dictionaryEntries[entry] = new byte[entryLength];
      for (int index = 0; index < entryLength; index++) {
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


  private int expandToBitWidth(DataPageInfo info) {
    int bitWidth = data[info.contentOffset()];
    if (bitWidth == 0) {
      return 0;
    } else {
      return 1 << (bitWidth - 1);
    }
  }

  @Deprecated
  private int expandToBitWidth() {
    int bitWidth = data[0];
    if (bitWidth == 0) {
      return 0;
    } else {
      return 1 << (bitWidth - 1);
    }
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

