package org.apache.parquet.parqour.ingest.ffreader;

import org.apache.parquet.column.ValuesType;
import org.apache.parquet.parqour.ingest.paging.DataPageMetadata;

/**
 * Created by sircodesalot on 6/21/15.
 */
@Deprecated
public abstract class DictionaryBasedFastForwardReader extends FastForwardReaderBase {
  public DictionaryBasedFastForwardReader(DataPageMetadata metadata, ValuesType type) {
    super(metadata, type);
  }

  public abstract int readNextDictionaryEntryIndex();
  public abstract byte[] getDictionaryEntryIndexAsBytes(int index);
}
