package org.apache.parquet.parqour.ingest.read.nodes;

import org.apache.parquet.parqour.ingest.read.nodes.categories.IngestNode;

import java.util.*;

/**
 * Created by sircodesalot on 6/17/15.
 */
public class IngestNodeSet implements Iterable<IngestNode> {
  private Set<IngestNode> ingestNodes = new HashSet<IngestNode>();
  private Map<String, IngestNode> ingestNodesByPath = new HashMap<String, IngestNode>();

  public IngestNodeSet() {

  }

  public IngestNodeSet(Iterable<IngestNode> ingestNodes) {
    addAll(ingestNodes);
  }

  public IngestNodeSet(IngestNode ... nodes) {
    addAll(nodes);
  }

  public void add(IngestNode node) {
    ingestNodes.add(node);
    ingestNodesByPath.put(node.path(), node);
  }

  public void addAll(IngestNode ... nodes) {
    for (IngestNode node : nodes) {
      add(node);
    }
  }

  public void addAll(Iterable<IngestNode> nodes) {
    for (IngestNode node : nodes) {
      add(node);
    }
  }

  public void remove(IngestNode node) {
    ingestNodes.remove(node);
    ingestNodesByPath.remove(node.path());
  }

  public void removeAll(IngestNode ... nodes) {
    for (IngestNode node : nodes) {
      remove(node);
    }
  }

  public void removeAll(Iterable<IngestNode> nodes) {
    for (IngestNode node : nodes) {
      remove(node);
    }
  }

  public boolean containsPath(String path) { return this.ingestNodesByPath.containsKey(path); }
  public Iterable<IngestNode> ingestNodes() { return this.ingestNodes; }
  public int size() { return this.ingestNodesByPath.size(); }

  @Override
  public Iterator<IngestNode> iterator() {
    return ingestNodes.iterator();
  }
}
