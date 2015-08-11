package org.apache.parquet.parqour.ingest.disk.pages.meta;

import org.apache.parquet.parqour.ingest.disk.pages.queue.BlockPageSetQueue;

import java.util.Iterator;

/**
 * Created by sircodesalot on 8/11/15.
 */
public class PageMetaIterable implements Iterable<PageMeta> {
  public final BlockPageSetQueue pageSetQueue;

  public PageMetaIterable(BlockPageSetQueue pageSetQueue) {
    this.pageSetQueue = pageSetQueue;
  }

  @Override
  public Iterator<PageMeta> iterator() {
    return new PageMetaIterator(pageSetQueue);
  }
}
