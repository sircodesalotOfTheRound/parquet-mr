package org.apache.parquet.parqour.ingest.disk.pages.queue;

import org.apache.parquet.parqour.ingest.disk.pagesets.RowGroupColumnPageSetInfo;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

/**
 * Created by sircodesalot on 8/11/15.
 */
public class BlockPageSetQueue implements Iterable<RowGroupColumnPageSetInfo> {
  private final Queue<RowGroupColumnPageSetInfo> pageSets = new ArrayDeque<RowGroupColumnPageSetInfo>();

  public void add(RowGroupColumnPageSetInfo columnInfo) {
    pageSets.add(columnInfo);
  }

  @Override
  public Iterator<RowGroupColumnPageSetInfo> iterator() {
    return pageSets.iterator();
  }
}
