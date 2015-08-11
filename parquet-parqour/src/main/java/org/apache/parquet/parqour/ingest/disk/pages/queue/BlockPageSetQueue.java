package org.apache.parquet.parqour.ingest.disk.pages.queue;

import org.apache.parquet.parqour.ingest.disk.pagesets.RowGroupPageSetColumnInfo;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

/**
 * Created by sircodesalot on 8/11/15.
 */
public class BlockPageSetQueue implements Iterable<RowGroupPageSetColumnInfo> {
  private final Queue<RowGroupPageSetColumnInfo> pageSets = new ArrayDeque<RowGroupPageSetColumnInfo>();

  public void add(RowGroupPageSetColumnInfo columnInfo) {
    pageSets.add(columnInfo);
  }

  @Override
  public Iterator<RowGroupPageSetColumnInfo> iterator() {
    return pageSets.iterator();
  }
}
