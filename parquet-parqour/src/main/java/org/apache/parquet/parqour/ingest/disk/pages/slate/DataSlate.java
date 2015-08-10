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
  private long endingOffset = 0;

  private final FSDataInputStream stream;
  private boolean isBuilt = false;
  private byte[] data;


  public DataSlate(HDFSParquetFile file, long startingOffset) {
    this.startingOffset = startingOffset;
    this.stream = file.stream();
  }

  public void addSegment(FSDataInputStream stream, PageHeader header) {
    if (!isBuilt) {
      this.endingOffset += computeEndingOffset(stream, header);
    } else {
      throw new DataIngestException("Data Slate is already built.");
    }
  }

  public byte[] data() {
    if (isBuilt) {
      return data;
    } else {
      throw new DataIngestException("Data Slate is not yet built");
    }
  }

  public void construct() {
    try {
      int totalSize = (int)(endingOffset - startingOffset);
      byte[] data = new byte[totalSize];

      stream.readFully(startingOffset, data);
      this.isBuilt = true;
    } catch (IOException ex) {
      throw new DataIngestException("Unable to read pages for file");
    }
  }

  private long computeEndingOffset(FSDataInputStream stream, PageHeader header) {
    try {
      return stream.getPos() + header.getCompressed_page_size();
    } catch (IOException ex) {
      throw new DataIngestException("Unable to read pages for file.");
    }
  }
}
