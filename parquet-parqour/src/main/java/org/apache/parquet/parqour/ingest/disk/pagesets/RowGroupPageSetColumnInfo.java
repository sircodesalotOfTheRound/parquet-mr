package org.apache.parquet.parqour.ingest.disk.pagesets;

import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFile;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFileMetadata;
import org.apache.parquet.parqour.ingest.disk.pages.info.DictionaryPageInfo;
import org.apache.parquet.parqour.ingest.disk.pages.meta.pagemetas.PageMeta;
import org.apache.parquet.schema.PrimitiveType;

import java.util.Iterator;

/**
 * Created by sircodesalot on 8/10/15.
 */
public class RowGroupPageSetColumnInfo implements Iterable<PageMeta> {
  private final HDFSParquetFile file;
  private final ColumnChunkMetaData column;
  private final HDFSParquetFileMetadata metadata;
  private final String path;

  private DictionaryPageInfo dictionaryPage;

  public RowGroupPageSetColumnInfo(HDFSParquetFile file, HDFSParquetFileMetadata metadata, ColumnChunkMetaData column) {
    this.file = file;
    this.column = column;
    this.metadata = metadata;
    this.path = column.getPath().toDotString();
  }

  @Override
  public Iterator<PageMeta> iterator() {
    return new ColumnPageSetIterator(this, file, metadata);
  }

  public DictionaryPageInfo dictionaryPage() { return this.dictionaryPage; }
  public CompressionCodecName codec() { return column.getCodec(); }
  public long totalEntryCount() { return column.getValueCount(); }
  public long dictionaryPageOffset() { return column.getDictionaryPageOffset(); }
  public long size() { return column.getTotalSize(); }
  public long uncompressedSize() { return column.getTotalUncompressedSize(); }
  public Statistics statistics() { return column.getStatistics(); }
  public long startingOffset() { return column.getStartingPos(); }
  public String path() { return path; }
  public PrimitiveType.PrimitiveTypeName type() { return column.getType(); }
}
