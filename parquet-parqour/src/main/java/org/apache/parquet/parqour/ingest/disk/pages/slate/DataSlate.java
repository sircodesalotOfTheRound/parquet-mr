package org.apache.parquet.parqour.ingest.disk.pages.slate;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFile;

import java.io.IOException;

/**
 * Created by sircodesalot on 8/10/15.
 */
public class DataSlate {
  private final long startingOffset;
  private long finalOffset = 0;

  private final FSDataInputStream stream;
  private boolean isBuilt = false;
  private byte[] data;


  public DataSlate(HDFSParquetFile file, long startingOffset) {
    this.startingOffset = startingOffset;
    this.stream = file.stream();
  }

  public int addSegment(PageHeader header) {
    if (!isBuilt) {
      int startOfPageOffset = computeStartOfPageOffset();
      this.finalOffset = computeEndingOffset(header);
      return startOfPageOffset;
    } else {
      throw new DataIngestException("Data Slate is already built.");
    }
  }

  public byte[] data() {
    if (!isBuilt) {
      this.data = this.construct();
      this.isBuilt = true;
    }

    return data;
  }

  private byte[] construct() {
    try {
      int totalSize = (int)(finalOffset - startingOffset);
      byte[] data = new byte[totalSize];

      stream.readFully(startingOffset, data);
      this.isBuilt = true;

      return data;
    } catch (IOException ex) {
      throw new DataIngestException(ex, "Unable to read pages for file");
    }
  }

  private int computeStartOfPageOffset() {
    try {
      return (int)(stream.getPos() - startingOffset);
    } catch (IOException ex) {
      throw new DataIngestException(ex, "Unable to read pages for file.");
    }
  }

  private long computeEndingOffset(PageHeader header) {
    try {
      return (stream.getPos() + header.getCompressed_page_size());
    } catch (IOException ex) {
      throw new DataIngestException(ex, "Unable to read pages for file.");
    }
  }

  public long startingOffset() { return startingOffset; }

}
