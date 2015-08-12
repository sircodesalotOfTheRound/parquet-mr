package org.apache.parquet.parqour.ingest.disk.pages;

import org.apache.parquet.parqour.ingest.disk.pages.queue.BlockPageSetQueue;

import java.util.Iterator;

/**
 * Created by sircodesalot on 8/12/15.
 */
public class Pager implements Iterable<Page> {
  private final BlockPageSetQueue blockChain;

  public Pager(BlockPageSetQueue blockChain) {
    this.blockChain = blockChain;
  }

  @Override
  public Iterator<Page> iterator() {
    return new PageIterator(blockChain);
  }
}
