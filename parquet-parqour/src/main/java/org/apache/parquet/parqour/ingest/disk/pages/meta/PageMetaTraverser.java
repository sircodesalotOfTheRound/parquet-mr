package org.apache.parquet.parqour.ingest.disk.pages.meta;

import org.apache.parquet.parqour.ingest.disk.pages.queue.BlockPageSetQueue;

import java.util.Iterator;

/**
 * Created by sircodesalot on 8/12/15.
 */
public class PageMetaTraverser implements Iterator<PageMeta> {
  private final Iterator<PageMeta> iterator;
  private long currentEntryNumber;

  public PageMetaTraverser(BlockPageSetQueue blockChain) {
    this.iterator = new PageMetaIterable(blockChain).iterator();
    this.currentEntryNumber = 0;
  }

  public long currentEntryNumber() { return this.currentEntryNumber; }

  public boolean hasNext() {
    return iterator.hasNext();
  }

  public PageMeta next() {
    PageMeta pageMeta = iterator.next();
    this.currentEntryNumber += pageMeta.totalEntryCount();
    return pageMeta;
  }
}
