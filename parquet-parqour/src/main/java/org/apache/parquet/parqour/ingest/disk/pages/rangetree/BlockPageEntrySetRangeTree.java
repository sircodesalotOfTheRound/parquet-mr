package org.apache.parquet.parqour.ingest.disk.pages.rangetree;

import org.apache.parquet.parqour.ingest.disk.pagesets.RowGroupColumnPageSetInfo;

/**
 * Created by sircodesalot on 8/11/15.
 */
public class BlockPageEntrySetRangeTree {
  class Node {
    private long startingEntryIndex;
    private long endingEntryIndex;

    private RowGroupColumnPageSetInfo columnInfo;

    public Node(RowGroupColumnPageSetInfo columnInfo, long startingEntryIndex) {
      this.columnInfo = columnInfo;
      this.startingEntryIndex = startingEntryIndex;
      this.endingEntryIndex = startingEntryIndex + columnInfo.totalEntryCount();
    }
  }

  private Node head;

  public void insert(RowGroupColumnPageSetInfo columnInfo) {

  }
}
