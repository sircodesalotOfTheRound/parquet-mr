package org.apache.parquet.parqour.ingest.disk.blocks;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFile;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFileMetadata;
import org.apache.parquet.parqour.ingest.disk.pages.DictionaryPageInfo;
import org.apache.parquet.parqour.ingest.disk.pages.PageInfo;
import org.apache.parquet.parqour.tools.TransformCollection;
import org.apache.parquet.parqour.tools.TransformList;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;

/**
 * Created by sircodesalot on 8/10/15.
 */
public class RowGroupColumnInfo {
  private final HDFSParquetFile file;
  private final ColumnChunkMetaData column;
  private final HDFSParquetFileMetadata metadata;

  private DictionaryPageInfo dictionaryPage;
  private TransformCollection<PageInfo> pages;

  public RowGroupColumnInfo(HDFSParquetFile file, HDFSParquetFileMetadata metadata, ColumnChunkMetaData column) {
    this.file = file;
    this.column = column;
    this.metadata = metadata;
  }

  public TransformCollection<PageInfo> pages() {
    if (pages == null) {
      this.pages = readPages();
    }

    return this.pages;
  }

  private TransformCollection<PageInfo> readPages() {
    try {
      FSDataInputStream stream = file.stream();
      stream.seek(startingOffset());
      TransformList<PageInfo> pages = new TransformList<PageInfo>();

      long totalEntriesRead = 0;
      while (totalEntriesRead < totalEntryCount()) {
        PageHeader header = Util.readPageHeader(stream);
        PageInfo pageInfo = PageInfo.readPage(this, file, metadata, header);
        if (pageInfo.isDictionaryPage()) {
          this.dictionaryPage = (DictionaryPageInfo)pageInfo;
        } else {
          pages.add(pageInfo);
          totalEntriesRead += pageInfo.entryCount();
        }
      }

      return pages;
    } catch (IOException ex) {
      throw new DataIngestException("Unable to read pages for column: '%s'", path());
    }
  }

  public DictionaryPageInfo dictionaryPage() { return this.dictionaryPage; }
  public CompressionCodecName codec() { return column.getCodec(); }
  public long totalEntryCount() { return column.getValueCount(); }
  public long dictionaryPageOffset() { return column.getDictionaryPageOffset(); }
  public long size() { return column.getTotalSize(); }
  public long uncompressedSize() { return column.getTotalUncompressedSize(); }
  public Statistics statistics() { return column.getStatistics(); }
  public long startingOffset() { return column.getStartingPos(); }
  public String path() { return column.getPath().toDotString(); }
  public PrimitiveType.PrimitiveTypeName type() { return column.getType(); }
}
