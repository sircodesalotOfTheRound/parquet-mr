package org.apache.parquet.parqour.ingest.paging;

import org.apache.parquet.parqour.ingest.ffreader.interfaces.FastForwardReader;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DictionaryPage;

/**
* Created by sircodesalot on 6/3/15.
*/
public class DataPageDecorator {
  private final DataPageMetadata metadata;
  private final ReaderSet readers;

  public DataPageDecorator(DataPage page, DictionaryPage dictionaryPage, ColumnDescriptor columnDescriptor, int startingRowNumber) {
    this.metadata = new DataPageMetadata(page, dictionaryPage, columnDescriptor, startingRowNumber);
    this.readers = new ReaderSet(metadata);
  }

  public long totalItems() { return this.metadata.totalItems(); }
  public DataPage page() { return this.metadata.page(); }
  public ColumnDescriptor columnDescriptor() { return this.metadata.columnDescriptor(); }

  public <T extends FastForwardReader> T definitionLevelReader() { return (T)this.readers.definitionLevelReader(); }
  public <T extends FastForwardReader> T repetitionLevelReader() { return (T)this.readers.repetitionLevelReader(); }
  public <T extends FastForwardReader> T valuesReader() { return (T) this.readers.valuesReader(); }

  public byte[] data() { return this.metadata.data(); }
  public DictionaryPage dictionaryPage() { return this.metadata.dictionaryPage(); }
  public int startingEntryNumber() { return this.metadata.startingEntryNumber(); }
  public int finalRowNumber() { return this.metadata.finalEntryNumber(); }
  public boolean containsRow(int rowNumber) { return this.metadata.entryNumber(rowNumber); }
  public DataPageMetadata metadata() { return this.metadata; }
}
