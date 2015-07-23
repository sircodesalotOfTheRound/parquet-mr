package org.apache.parquet.parqour.ingest.ffreader.dictionary;

import org.apache.parquet.column.ValuesType;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.ingest.ffreader.DictionaryBasedFastForwardReader;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.BinaryFastForwardReader;
import org.apache.parquet.parqour.ingest.ffreader.segments.PackedEncodingSegmentReader;
import org.apache.parquet.parqour.ingest.paging.DataPageMetadata;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by sircodesalot on 6/13/15.
 */
public final class PlainBinaryDictionaryFastForwardReader extends DictionaryBasedFastForwardReader
  implements BinaryFastForwardReader
{
  private PackedEncodingSegmentReader segment;

  private byte[][] dictionaryEntries;
  private String[] dictionaryEntriesAsStrings;
  private byte[] dictionaryPageData;
  private int dictionaryPageOffset = -1;

  public PlainBinaryDictionaryFastForwardReader(DataPageMetadata metadata, ValuesType type) {
    super(metadata, type);

    this.dictionaryPageData = collectDictionaryPageData(metadata.dictionaryPage());
    this.dictionaryEntries = this.readDictionaryEntries(metadata.dictionaryPage());
    this.dictionaryEntriesAsStrings = new String[metadata.dictionaryPage().getDictionarySize()];
    this.segment = PackedEncodingSegmentReader.createPackedEncodingSegmentReader(data, 0, 10);
  }

  private byte[] collectDictionaryPageData(DictionaryPage dictionaryPage) {
    try {
      return dictionaryPage.getBytes().toByteArray();
    } catch (IOException ex) {
      throw new DataIngestException("Unable to load dictionary page.");
    }
  }

  private byte[][] readDictionaryEntries(DictionaryPage dictionaryPage) {
    byte[][] entries = new byte[dictionaryPage.getDictionarySize()][];
    for (int index = 0; index < dictionaryPage.getDictionarySize(); index++) {
      int length = readDictionaryPageData();
      entries[index] = Arrays.copyOfRange(dictionaryPageData,
        dictionaryPageOffset + 1,
        dictionaryPageOffset + length + 1);

      dictionaryPageOffset += length;
    }

    return entries;
  }

  private int readDictionaryPageData() {
    return (dictionaryPageData[++dictionaryPageOffset] & 0xFF)
      | (dictionaryPageData[++dictionaryPageOffset] & 0xFF) << 8
      | (dictionaryPageData[++dictionaryPageOffset] & 0xFF) << 16
      | (dictionaryPageData[++dictionaryPageOffset] & 0xFF) << 24;
  }

  @Deprecated
  public int readNextDictionaryEntryIndex() {
    throw new NotImplementedException();
  }

  @Deprecated
  public byte[] getDictionaryEntryIndexAsBytes(int index) {
    throw new NotImplementedException();
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

