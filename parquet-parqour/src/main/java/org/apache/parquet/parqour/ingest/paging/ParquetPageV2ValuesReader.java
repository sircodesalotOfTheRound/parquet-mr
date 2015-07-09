package org.apache.parquet.parqour.ingest.paging;

import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Created by sircodesalot on 6/10/15.
 */
@Deprecated
public class ParquetPageV2ValuesReader extends ValuesReader {
  private final RunLengthBitPackingHybridDecoder decoder;

  public ParquetPageV2ValuesReader(int maxLevel, byte[] data) {
      this.decoder = new RunLengthBitPackingHybridDecoder(
        BytesUtils.getWidthFromMaxInt(maxLevel),
        new ByteArrayInputStream(data));
  }

  @Override
  public void initFromPage(int i, byte[] bytes, int i1) throws IOException {

  }

  @Override
  public int getNextOffset() {
    try {
      return decoder.readInt();
    } catch (IOException ex) {
      throw new DataIngestException("Unable to read next entry");
    }
  }

  @Override
  public void skip() {
    try {
      this.decoder.readInt();
    } catch (IOException ex) {
      throw new DataIngestException("Unable to read next entry");
    }
  }
}
