package org.apache.parquet.parqour.ingest.ffreader.interfaces;

/**
 * Created by sircodesalot on 6/24/15.
 */
public interface PeekableFastForwardReader extends FastForwardReader {
  boolean any();
  int peek();
}
