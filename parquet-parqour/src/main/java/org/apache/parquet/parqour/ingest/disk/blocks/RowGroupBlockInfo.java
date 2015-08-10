package org.apache.parquet.parqour.ingest.disk.blocks;

import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFile;
import org.apache.parquet.parqour.ingest.read.iterator.lamba.Projection;
import org.apache.parquet.parqour.query.expressions.predicate.TextQueryTestablePredicateExpression;
import org.apache.parquet.parqour.tools.TransformList;
import org.apache.parquet.parqour.tools.TransformCollection;

/**
 * Created by sircodesalot on 8/10/15.
 */
public class RowGroupBlockInfo {
  private final HDFSParquetFile file;
  private final BlockMetaData metadata;

  public RowGroupBlockInfo(HDFSParquetFile file, BlockMetaData metadata) {
    this.file = file;
    this.metadata = metadata;
  }

  public boolean shouldRead(TextQueryTestablePredicateExpression predicate) {
    return true;
  }

  public TransformCollection<RowGroupColumnInfo> columnMetadata() {
    return new TransformList<ColumnChunkMetaData>(this.metadata.getColumns())
      .map(new Projection<ColumnChunkMetaData, RowGroupColumnInfo>() {
        @Override
        public RowGroupColumnInfo apply(ColumnChunkMetaData column) {
          return new RowGroupColumnInfo(file, column);
        }
      });
  }

  public long compressedSize() { return metadata.getCompressedSize(); }
  public long rowCount() { return metadata.getRowCount(); }
  public long startingOffset() { return metadata.getStartingPos(); }
  public long totalBytes() { return metadata.getTotalByteSize(); }
}
