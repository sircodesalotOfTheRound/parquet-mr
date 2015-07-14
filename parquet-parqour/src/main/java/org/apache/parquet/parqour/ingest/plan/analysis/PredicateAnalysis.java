package org.apache.parquet.parqour.ingest.plan.analysis;

import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.ColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.builders.ColumnPredicateBuildable;
import org.apache.parquet.parqour.ingest.plan.predicates.types.ColumnPredicateNodeCategory;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;

import java.util.*;

/**
 * Created by sircodesalot on 6/1/15.
 */
public class PredicateAnalysis {
  private final ColumnPredicate finalPredicate;
  private final Set<ColumnPredicate.LeafColumnPredicate> leaves;
  private final Map<String, ColumnPredicate.LeafColumnPredicate> leavesByColumnPath;
  private final PredicateAnalysisColumnSet columns;

  public PredicateAnalysis(IngestTree ingestTree) {
    this.finalPredicate = analyzePredicate(ingestTree);
    this.leaves = captureLeaves(finalPredicate);
    this.columns = new PredicateAnalysisColumnSet(leaves);
    this.leavesByColumnPath = captureLeavesByColumnPath(leaves);
  }

  private ColumnPredicate analyzePredicate(IngestTree ingestTree) {
    SchemaInfo schemaInfo = ingestTree.schemaInfo();
    FilterPredicate predicate = schemaInfo.predicate();
    if (predicate != null) {
      PredicateAnalysisVisitor visitor = new PredicateAnalysisVisitor(ingestTree);
      ColumnPredicateBuildable analyzedPredicate = predicate.accept(visitor);

      return analyzedPredicate.build(null, ingestTree);

    } else {
      return ColumnPredicate.NONE;
    }
  }

  private Set<ColumnPredicate.LeafColumnPredicate> captureLeaves(ColumnPredicate predicate) {
    Set<ColumnPredicate.LeafColumnPredicate> leaves = new HashSet<ColumnPredicate.LeafColumnPredicate>();
    Queue<ColumnPredicate> fringe = new ArrayDeque<ColumnPredicate>();

    // BFS
    fringe.add(predicate);
    while (!fringe.isEmpty()) {
      ColumnPredicate current = fringe.remove();

      if (current.nodeCategory() == ColumnPredicateNodeCategory.SYSTEM_DEFINED_LEAF
        || current.nodeCategory() == ColumnPredicateNodeCategory.USER_DEFINED_LEAF) {

        leaves.add((ColumnPredicate.LeafColumnPredicate) current);
      } else {

        fringe.add(((ColumnPredicate.LogicColumnPredicate) current).lhs());
        fringe.add(((ColumnPredicate.LogicColumnPredicate) current).rhs());
      }
    }

    return leaves;
  }

  private Map<String, ColumnPredicate.LeafColumnPredicate> captureLeavesByColumnPath(Iterable<ColumnPredicate.LeafColumnPredicate> leaves) {
    Map<String, ColumnPredicate.LeafColumnPredicate> leavesByColumnPath = new HashMap<String, ColumnPredicate.LeafColumnPredicate>();
    for (ColumnPredicate.LeafColumnPredicate leaf : leaves) {
      leavesByColumnPath.put(leaf.columnPath().toDotString(), leaf);
    }
    return leavesByColumnPath;
  }

  @Deprecated
  public ColumnPredicate.LeafColumnPredicate getPredicateForLeaf(String path) {
    if (leavesByColumnPath.containsKey(path)) {
      return leavesByColumnPath.get(path);
    } else {
      return ColumnPredicate.NONE;
    }
  }

  public ColumnPredicate predicateTree() { return this.finalPredicate; }
  public Iterable<ColumnPredicate.LeafColumnPredicate> leaves() { return this.leaves; }
  public PredicateAnalysisColumnSet columns() { return this.columns; }
}
