package org.apache.parquet.parqour.ingest.disk.blocks;

import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFile;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFileMetadata;
import org.apache.parquet.parqour.ingest.disk.pagesets.RowGroupPageSetColumnInfo;
import org.apache.parquet.parqour.ingest.read.iterator.lamba.Projection;
import org.apache.parquet.parqour.query.expressions.predicate.TextQueryTestablePredicateExpression;
import org.apache.parquet.parqour.tools.TransformList;
import org.apache.parquet.parqour.tools.TransformCollection;

/**
 * Created by sircodesalot on 8/10/15.
 */
public class RowGroupBlockInfo {
  private final HDFSParquetFile file;
  private final BlockMetaData blockMetadata;
  private final HDFSParquetFileMetadata metadata;

  public RowGroupBlockInfo(HDFSParquetFile file, HDFSParquetFileMetadata metadata, BlockMetaData blockMetadata) {
    this.file = file;
    this.metadata = metadata;
    this.blockMetadata = blockMetadata;
  }

  public boolean shouldRead(TextQueryTestablePredicateExpression predicate) {
    return true;
  }

  public TransformCollection<RowGroupPageSetColumnInfo> columnMetadata() {
    return new TransformList<ColumnChunkMetaData>(this.blockMetadata.getColumns())
      .map(new Projection<ColumnChunkMetaData, RowGroupPageSetColumnInfo>() {
        @Override
        public RowGroupPageSetColumnInfo apply(ColumnChunkMetaData column) {
          return new RowGroupPageSetColumnInfo(file, metadata, column);
        }
      });
  }

  public long compressedSize() { return blockMetadata.getCompressedSize(); }
  public long rowCount() { return blockMetadata.getRowCount(); }
  public long startingOffset() { return blockMetadata.getStartingPos(); }
  public long totalBytes() { return blockMetadata.getTotalByteSize(); }
}
