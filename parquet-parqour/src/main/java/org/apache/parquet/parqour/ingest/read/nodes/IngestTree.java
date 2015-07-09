package org.apache.parquet.parqour.ingest.read.nodes;

import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.ingest.paging.DiskInterfaceManager;
import org.apache.parquet.parqour.ingest.read.nodes.categories.AggregatingIngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.categories.IngestNode;
import org.apache.parquet.parqour.ingest.read.nodes.categories.IngestNodeCategory;
import org.apache.parquet.parqour.ingest.read.nodes.impl.RootIngestNode;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;

import java.util.*;

/**
 * Created by sircodesalot on 6/9/15.
 */
public class IngestTree {
  private final SchemaInfo schemaInfo;
  private final RootIngestNode root;
  private final Map<String, IngestNode> ingestNodesByPath;
  private final Iterable<IngestNode> ingestNodeList;

  public IngestTree(SchemaInfo schemaInfo, DiskInterfaceManager diskInterfaceManager) {
    this.schemaInfo = schemaInfo;
    this.root = new RootIngestNode(schemaInfo, diskInterfaceManager);
    this.ingestNodesByPath = collectIngestNodesByPath(root);
    this.ingestNodeList = collectIngestNodes(ingestNodesByPath);
  }

  private Map<String, IngestNode> collectIngestNodesByPath(IngestNode root) {
    return collectIngestNodesByPath(root, new HashMap<String, IngestNode>());
  }

  private Map<String, IngestNode> collectIngestNodesByPath(IngestNode node, Map<String, IngestNode> table) {
    table.put(node.path(), node);
    if (node.category() == IngestNodeCategory.AGGREGATOR) {
      for (IngestNode child : ((AggregatingIngestNode)node).children()) {
        collectIngestNodesByPath(child, table);
      }
    }

    return table;
  }

  private Iterable<IngestNode> collectIngestNodes(Map<String, IngestNode> ingestNodesByPath) {
    List<IngestNode> ingestNodeList = new ArrayList<IngestNode>();
    for (String columnPath : ingestNodesByPath.keySet()) {
      ingestNodeList.add(ingestNodesByPath.get(columnPath));
    }

    return ingestNodeList;
  }

  public IngestNode getIngestNodeByPath(String path) {
    if (ingestNodesByPath.containsKey(path)) {
      return ingestNodesByPath.get(path);
    } else {
      throw new DataIngestException("Unable to find node with path: %s", path);
    }
  }

  public IngestNodeSet collectIngestNodeDependenciesForPaths(String... paths) {
    Set<IngestNode> depdendencies = new HashSet<IngestNode>();
    for (String path : paths) {
      IngestNode node = getIngestNodeByPath(path);
      depdendencies = findIngestNodeDependencies(node, depdendencies);
    }

    return new IngestNodeSet(depdendencies);
  }

  private Set<IngestNode> findIngestNodeDependencies(IngestNode node, Set<IngestNode> dependencies) {
    if (node.category() == IngestNodeCategory.DATA_INGEST) {
      dependencies.add(node);
    } else {
      AggregatingIngestNode groupNode = (AggregatingIngestNode)node;
      for (IngestNode child : groupNode.children()) {
        findIngestNodeDependencies(child, dependencies);
      }
    }

    return dependencies;
  }

  public SchemaInfo schemaInfo() { return this.schemaInfo; }
  public RootIngestNode root() { return this.root; }
  public Iterable<IngestNode> ingestNodes() { return this.ingestNodeList; }

}
