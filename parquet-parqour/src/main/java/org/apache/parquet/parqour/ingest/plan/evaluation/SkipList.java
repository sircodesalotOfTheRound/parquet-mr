package org.apache.parquet.parqour.ingest.plan.evaluation;

import org.apache.parquet.parqour.ingest.read.nodes.categories.IngestNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by sircodesalot on 6/17/15.
 */
@Deprecated
public class SkipList implements Iterable<IngestNode> {
  private final List<IngestNode> skippedNodes = new ArrayList<IngestNode>();

  @Deprecated
  public void add(IngestNode[] nodes) {
    for (IngestNode ingestNode : nodes) {
      skippedNodes.add(ingestNode);
    }
  }

  public void clear() {
    this.skippedNodes.clear();
  }

  @Override
  public Iterator<IngestNode> iterator() {
    return skippedNodes.iterator();
  }
}
