package org.apache.parquet.parqour.ingest.disk;

import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFile;

/**
 * Created by sircodesalot on 8/10/15.
 */
public class BlockReader {
  private final org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFile HDFSParquetFile;

  public BlockReader(HDFSParquetFile HDFSParquetFile) {
    this.HDFSParquetFile = HDFSParquetFile;
    //this.metadata = readMetadata(HDFSParquetFile);
  }

}
