package org.apache.parquet.parqour.ingest.disk.manager;

import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.ingest.disk.files.HDFSParquetFileMetadata;
import org.apache.parquet.parqour.ingest.disk.pages.meta.PageMetaTraverser;
import org.apache.parquet.parqour.ingest.disk.pages.queue.BlockPageSetQueue;
import org.apache.parquet.parqour.ingest.disk.blocks.RowGroupBlockInfo;
import org.apache.parquet.parqour.ingest.disk.pagesets.RowGroupPageSetColumnInfo;
import org.apache.parquet.parqour.tools.TransformCollection;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by sircodesalot on 8/11/15.
 */
public class DiskInterfaceManager {
  private final HDFSParquetFileMetadata metadata;
  private final Map<String, BlockPageSetQueue> pageSetsByPath;

  public DiskInterfaceManager(HDFSParquetFileMetadata metadata) {
    this.metadata = metadata;
    this.pageSetsByPath = collectPageSets(metadata.blocks());
  }

  private Map<String, BlockPageSetQueue> collectPageSets(TransformCollection<RowGroupBlockInfo> blocks) {
    Map<String, BlockPageSetQueue> queues = new HashMap<String, BlockPageSetQueue>();

    for (RowGroupBlockInfo block : blocks) {
      for (RowGroupPageSetColumnInfo column : block.columnMetadata()) {
        if (!queues.containsKey(column.path())) {
          queues.put(column.path(), new BlockPageSetQueue());
        }

        BlockPageSetQueue pageQueue = queues.get(column.path());
        pageQueue.add(column);
      }
    }

    return queues;
  }

  public boolean containsPageSetQueueForColumn(String path) {
    return pageSetsByPath.containsKey(path);
  }

  public PageMetaTraverser generatePageTraverserForPath(String path) {
    if (pageSetsByPath.containsKey(path)) {
      BlockPageSetQueue blockChain = pageSetsByPath.get(path);
      return new PageMetaTraverser(blockChain);
    } else {
      throw new DataIngestException("No Page-Queue for Path");
    }
  }
}
