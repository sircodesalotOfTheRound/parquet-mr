package org.apache.parquet.parqour.ingest.disk.pages;

import org.apache.parquet.parqour.ingest.disk.pages.meta.pagemetas.PageMeta;
import org.apache.parquet.parqour.ingest.disk.pages.meta.pagemetas.PageMetaIterable;
import org.apache.parquet.parqour.ingest.disk.pages.queue.BlockPageSetQueue;

import java.util.Iterator;

/**
 * Created by sircodesalot on 8/12/15.
 */
public class PageIterator implements Iterator<Page> {
  private final Iterator<PageMeta> iterator;
  private long currentEntryNumber;

  public PageIterator(BlockPageSetQueue blockChain) {
    this.iterator = new PageMetaIterable(blockChain).iterator();
    this.currentEntryNumber = 0;
  }

  public long currentEntryNumber() { return this.currentEntryNumber; }

  public boolean hasNext() {
    return iterator.hasNext();
  }

  public Page next() {
    PageMeta pageMeta = iterator.next();
    Page page = new Page(pageMeta, currentEntryNumber);
    this.currentEntryNumber += pageMeta.totalEntryCount();

    return page;
  }
}
