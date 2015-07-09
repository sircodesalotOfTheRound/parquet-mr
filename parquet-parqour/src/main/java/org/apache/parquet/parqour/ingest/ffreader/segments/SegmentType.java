package org.apache.parquet.parqour.ingest.ffreader.segments;

/**
 * Created by sircodesalot on 6/17/15.
 */
public enum SegmentType {
  ZERO,
  RUN_LENGTH_ENCODING,
  BIT_PACKED,
  DELTA_PACKED
}
