package org.apache.parquet.parqour.ingest.ffreader.delta;

import org.apache.parquet.parqour.ingest.ffreader.FastForwardReaderBase;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.Int32FastForwardReader;
import org.apache.parquet.parqour.ingest.ffreader.interfaces.Int64FastForwardReader;
import org.apache.parquet.parqour.ingest.ffreader.segments.DeltaPackedSegmentReader;
import org.apache.parquet.parqour.ingest.paging.DataPageMetadata;
import org.apache.parquet.column.ValuesType;

/**
 * Created by sircodesalot on 6/23/15.
 */
public class DeltaPackedIntegerFastForwardReader extends FastForwardReaderBase
  implements Int32FastForwardReader, Int64FastForwardReader {

  private final DeltaPackedSegmentReader deltaPackedSegment;

  public DeltaPackedIntegerFastForwardReader(DataPageMetadata metadata, ValuesType type) {
    super(metadata, type);

    this.deltaPackedSegment = new DeltaPackedSegmentReader(data);
  }

  @Override
  public int readi32() {
    super.advanceRowNumber();
    return deltaPackedSegment.readi32();
  }

  @Override
  public long readi64() {
    return deltaPackedSegment.readi64();
  }

  @Override
  public void fastForwardTo(int entryNumber) {
    for (long index = currentRow; index < entryNumber; index++) {
      deltaPackedSegment.readi64();
    }
  }
}
