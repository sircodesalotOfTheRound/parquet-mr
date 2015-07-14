package org.apache.parquet.parqour.ingest.paging;

import org.apache.parquet.column.ValuesType;
import org.apache.parquet.parqour.ingest.ffreader.FastForwardReaderBase;


/**
 * Created by sircodesalot on 6/10/15.
 */
public class ReaderSet {
  private final FastForwardReaderBase definitionLevelReader;
  private final FastForwardReaderBase repetitionLevelReader;
  private final FastForwardReaderBase valuesReader;

  public ReaderSet(DataPageMetadata metadata) {
    this.definitionLevelReader = FastForwardReaderBase.resolve(metadata, ValuesType.DEFINITION_LEVEL);
    this.repetitionLevelReader = FastForwardReaderBase.resolve(metadata, ValuesType.REPETITION_LEVEL);
    this.valuesReader = FastForwardReaderBase.resolve(metadata, ValuesType.VALUES);
  }

  public FastForwardReaderBase definitionLevelReader() { return this.definitionLevelReader; }
  public FastForwardReaderBase repetitionLevelReader() { return repetitionLevelReader; }
  public FastForwardReaderBase valuesReader() { return this.valuesReader; }
}
