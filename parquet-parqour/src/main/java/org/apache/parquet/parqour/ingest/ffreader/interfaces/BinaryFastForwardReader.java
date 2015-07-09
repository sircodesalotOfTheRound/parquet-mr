package org.apache.parquet.parqour.ingest.ffreader.interfaces;

/**
 * Created by sircodesalot on 6/25/15.
 */
public interface BinaryFastForwardReader extends FastForwardReader {
  String readString();
  byte[] readBytes();
}
