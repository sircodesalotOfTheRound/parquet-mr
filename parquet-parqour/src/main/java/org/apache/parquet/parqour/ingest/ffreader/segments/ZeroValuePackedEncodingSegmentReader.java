package org.apache.parquet.parqour.ingest.ffreader.segments;

/**
 * Created by sircodesalot on 6/15/15.
 */
public class ZeroValuePackedEncodingSegmentReader extends PackedEncodingSegmentReader {
  public static ZeroValuePackedEncodingSegmentReader INSTANCE = new ZeroValuePackedEncodingSegmentReader();

  @Override
  public boolean any() {
    return true;
  }

  @Override
  public PackedEncodingSegmentReader generateReaderForNextSection() {
    return this;
  }

  @Override
  public int readNext() {
    return 0;
  }
}
