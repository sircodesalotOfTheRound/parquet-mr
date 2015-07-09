package org.apache.parquet.parqour.ingest.plan.analysis;

import org.apache.parquet.parqour.ingest.plan.predicates.ColumnPredicate;
import org.apache.parquet.column.ColumnDescriptor;

import java.util.*;


/**
* Created by sircodesalot on 6/8/15.
*/
public class PredicateAnalysisColumnSet implements Iterable<ColumnDescriptor> {
  private final Set<ColumnDescriptor> columnDescriptors;
  private final Map<String, ColumnDescriptor> columnsByPath;

  public PredicateAnalysisColumnSet(Iterable<ColumnPredicate.LeafColumnPredicate> leaves) {
    this.columnDescriptors = captureColumnDescriptors(leaves);
    this.columnsByPath = collectColumnsByPath(leaves);
  }

  private Set<ColumnDescriptor> captureColumnDescriptors(Iterable<ColumnPredicate.LeafColumnPredicate> leaves) {
    Set<ColumnDescriptor> columnDescriptors = new HashSet<ColumnDescriptor>();
    for (ColumnPredicate.LeafColumnPredicate leaf : leaves) {
      columnDescriptors.add(leaf.column());
    }

    return columnDescriptors;
  }

  public Map<String, ColumnDescriptor> collectColumnsByPath(Iterable<ColumnPredicate.LeafColumnPredicate> leaves) {
    Map<String, ColumnDescriptor> paths = new HashMap<String, ColumnDescriptor>();
    for (ColumnPredicate.LeafColumnPredicate leaf : leaves) {
      paths.put(leaf.columnPath().toDotString(), leaf.column());
    }

    return paths;
  }

  public boolean containsColumn(String columnPath) {
    return columnsByPath.containsKey(columnPath);
  }

  public int size() { return this.columnDescriptors.size(); }

  @Override
  public Iterator<ColumnDescriptor> iterator() {
    return columnDescriptors.iterator();
  }
}
