package org.apache.parquet.parqour.ingest.disk.pages.meta;

import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.ingest.disk.pages.queue.BlockPageSetQueue;
import org.apache.parquet.parqour.ingest.disk.pagesets.RowGroupColumnPageSetInfo;

import java.util.Iterator;

/**
 * Created by sircodesalot on 8/11/15.
 */
public class PageMetaIterator implements Iterator<PageMeta> {
  private final Iterator<RowGroupColumnPageSetInfo> pageSetIterator;
  private Iterator<PageMeta> pageMetaIterator;

  public PageMetaIterator(BlockPageSetQueue pageSetQueue) {
    this.pageSetIterator = pageSetQueue.iterator();
    this.pageMetaIterator = intializePagingIterator(pageSetIterator);
  }

  private Iterator<PageMeta> intializePagingIterator(Iterator<RowGroupColumnPageSetInfo> pageSetIterator) {
    if (pageSetIterator.hasNext()) {
      return pageSetIterator.next().iterator();
    } else {
      return null;
    }
  }

  @Override
  public boolean hasNext() {
    if (pageMetaIterator == null) {
      return false;
    } else {
      return (pageMetaIterator.hasNext()) || (pageSetIterator.hasNext());
    }
  }

  @Override
  public PageMeta next() {
    return update();
  }

  private PageMeta update() {
    if (!pageMetaIterator.hasNext()) {
      if (pageSetIterator.hasNext()) {
        pageMetaIterator = pageSetIterator.next().iterator();
      } else {
        throw new DataIngestException("Read past the end of file");
      }
    }

    return pageMetaIterator.next();
  }
}
