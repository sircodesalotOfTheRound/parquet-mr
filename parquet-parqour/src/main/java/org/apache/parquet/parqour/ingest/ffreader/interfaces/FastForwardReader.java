package org.apache.parquet.parqour.ingest.ffreader.interfaces;

import org.apache.parquet.column.ValuesType;

/**
 * Created by sircodesalot on 6/24/15.
 */
public interface FastForwardReader {
  long totalItemsOnPage();

  ValuesType type();

  @Deprecated
  boolean isEof();

  void fastForwardTo(int entryNumber);
}
