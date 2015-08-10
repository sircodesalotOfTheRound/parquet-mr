package org.apache.parquet.parqour.ingest.disk.files;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.ingest.disk.blocks.RowGroupBlockInfo;
import org.apache.parquet.parqour.ingest.read.iterator.lamba.Projection;
import org.apache.parquet.parqour.tools.TransformList;
import org.apache.parquet.parqour.tools.TransformCollection;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by sircodesalot on 8/10/15.
 */
public class HDFSParquetFileMetadata {
  private static final String PAR1 = "PAR1";
  private static final int PAR1_LENGTH = PAR1.length();
  private static final int FOOTER_LENGTH = 4;

  private final HDFSParquetFile file;
  private final ParquetMetadata metadata;
  private final Map<String, ColumnDescriptor> columnDescriptors;

  public HDFSParquetFileMetadata(HDFSParquetFile file) {
    this.file = file;
    this.metadata = readMetadata(file);
    this.columnDescriptors = collectColumnDescriptors(metadata);
  }

  private Map<String, ColumnDescriptor> collectColumnDescriptors(ParquetMetadata metadata) {
    Map<String, ColumnDescriptor> columnDescriptors = new HashMap<String, ColumnDescriptor>();
    for (ColumnDescriptor descriptor : metadata.getFileMetaData().getSchema().getColumns()) {
      ColumnPath path = ColumnPath.get(descriptor.getPath());
      columnDescriptors.put(path.toDotString(), descriptor);
    }

    return columnDescriptors;
  }

  public ParquetMetadata readMetadata(HDFSParquetFile file) {
    FSDataInputStream stream = file.stream();

    try {
      long footerLengthIndex = computeFooterLengthIndex(file);
      int footerLength = readFooterLength(file);

      this.validateEndOfFooter(file);

      long footerStartIndex = (footerLengthIndex - footerLength);
      if (footerStartIndex < PAR1_LENGTH || footerStartIndex >= footerLengthIndex) {
        throw new RuntimeException("corrupted file: the footer index is not within the file");
      }

      return parseMetadata(file, footerStartIndex);
    } catch (IOException ex) {
      throw new DataIngestException("Unable to read metadata for file '%s'", file.path());
    }
  }

  private long computeFooterLengthIndex(HDFSParquetFile file) {
    long fileLength = readFileLength(file);
    return (fileLength - FOOTER_LENGTH) - PAR1.length();
  }

  private int readFooterLength(HDFSParquetFile file) throws IOException {
    FSDataInputStream stream = file.stream();

    long fileLength = readFileLength(file);
    long footerLengthIndex = (fileLength - FOOTER_LENGTH) - PAR1.length();

    stream.seek(footerLengthIndex);
    return (stream.readByte() & 0xFF)
      | (stream.readByte() & 0xFF) << 8
      | (stream.readByte() & 0xFF) << 16
      | (stream.readByte() & 0xFF) << 24;
  }

  private long readFileLength(HDFSParquetFile file) {
    long fileLength = file.status().getLen();
    if (fileLength < PAR1_LENGTH + FOOTER_LENGTH + PAR1_LENGTH) {
      throw new DataIngestException("'%s' is not a Parquet file (too small)", file.path());
    }

    return fileLength;
  }

  private void validateEndOfFooter(HDFSParquetFile file) throws IOException {
    byte[] par1 = new byte[PAR1_LENGTH];
    file.stream().readFully(par1);
    String par1AsString = new String(par1);

    if (!par1AsString.equals(PAR1)) {
      throw new DataIngestException("'%s' is not a Parquet file. Expected 'PAR1' at tail but found: '%s'", file.path(),  Arrays.toString(par1));
    }
  }

  private ParquetMetadata parseMetadata(HDFSParquetFile file, long footerStartIndex) throws IOException {
    FSDataInputStream stream = file.stream();
    stream.seek(footerStartIndex);
    ParquetMetadataConverter converter = new ParquetMetadataConverter();
    return converter.readParquetMetadata(stream, ParquetMetadataConverter.NO_FILTER);
  }

  public TransformCollection<RowGroupBlockInfo> blocks() {
    return new TransformList<BlockMetaData>(metadata.getBlocks())
      .map(new Projection<BlockMetaData, RowGroupBlockInfo>() {
        @Override
        public RowGroupBlockInfo apply(BlockMetaData blockMetaData) {
          return new RowGroupBlockInfo(file, HDFSParquetFileMetadata.this, blockMetaData);
        }
      });
  }

  public ColumnDescriptor getColumnDescriptor(String path) {
    if (columnDescriptors.containsKey(path)) {
      return columnDescriptors.get(path);
    } else {
      throw new DataIngestException("No ColumnDescriptor for path: '%s'", path);
    }
  }

  public FileMetaData fileMetaData() { return metadata.getFileMetaData(); }
  public String createdBy() { return metadata.getFileMetaData().getCreatedBy(); }

}
