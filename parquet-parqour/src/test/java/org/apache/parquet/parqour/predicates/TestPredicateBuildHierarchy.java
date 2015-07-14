package org.apache.parquet.parqour.predicates;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.parqour.ingest.plan.predicates.ColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.builders.leaf.sys.*;
import org.apache.parquet.parqour.ingest.plan.predicates.builders.logic.AndColumnPredicateBuilder;
import org.apache.parquet.parqour.ingest.plan.predicates.builders.logic.OrColumnPredicateBuilder;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by sircodesalot on 6/2/15.
 */
public class TestPredicateBuildHierarchy {
  private static final MessageType SINGLE_COLUMN_SCHEMA = new MessageType("single_column_schema",
    new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "numeric_column"));

  private static final ColumnDescriptor COLUMN = TestTools.getColumnFromSchema(SINGLE_COLUMN_SCHEMA, "numeric_column");
  private static final IngestTree INGEST_TREE = TestTools.generateIngestTreeFromSchema(SINGLE_COLUMN_SCHEMA);
  private static final Integer VALUE = 10;

  public static ColumnPredicate FIRST =
    new AndColumnPredicateBuilder(
      new OrColumnPredicateBuilder(
        new LessThanColumnPredicateBuilder(COLUMN, VALUE),
        new GreaterThanColumnPredicateBuilder(COLUMN, VALUE)),
      new EqualsColumnPredicateBuilder(COLUMN, VALUE)
    ).build(null, INGEST_TREE);

  public static ColumnPredicate SECOND =
    new OrColumnPredicateBuilder(
      new AndColumnPredicateBuilder(
        new EqualsColumnPredicateBuilder(COLUMN, VALUE),
        new EqualsColumnPredicateBuilder(COLUMN, VALUE)),
      new OrColumnPredicateBuilder(
        new EqualsColumnPredicateBuilder(COLUMN, VALUE),
        new NotEqualsColumnPredicateBuilder(COLUMN, VALUE)
      )
    ).build(null, INGEST_TREE);

  public static ColumnPredicate THIRD =
    new AndColumnPredicateBuilder(
      new AndColumnPredicateBuilder(
        new GreaterThanOrEqualsColumnPredicateBuilder(COLUMN, VALUE),
        new LessThanOrEqualsColumnPredicateBuilder(COLUMN, VALUE)),
      new AndColumnPredicateBuilder(
        new GreaterThanOrEqualsColumnPredicateBuilder(COLUMN, VALUE),
        new AndColumnPredicateBuilder(
          new GreaterThanOrEqualsColumnPredicateBuilder(COLUMN, VALUE),
          new LessThanOrEqualsColumnPredicateBuilder(COLUMN, VALUE))
      )
    ).build(null, INGEST_TREE);

  private static final Set<ColumnPredicate> NODES_IN_FIRST = captureNodes(FIRST);
  private static final Set<ColumnPredicate> NODES_IN_SECOND = captureNodes(SECOND);
  private static final Set<ColumnPredicate> NODES_IN_THIRD = captureNodes(THIRD);

  public static Set<ColumnPredicate> captureNodes(ColumnPredicate root) {
    final Set<ColumnPredicate> results = new HashSet<ColumnPredicate>();
    breadthFirstSearch(root, new BFSCallback() {
      @Override
      public void perform(ColumnPredicate predicate) {
        results.add(predicate);
      }
    });

    return results;
  }

  public interface BFSCallback {
    void perform(ColumnPredicate predicate);
  }

  private static void breadthFirstSearch(ColumnPredicate root, BFSCallback callback) {
    Queue<ColumnPredicate> fringe = new ArrayDeque<ColumnPredicate>();

    // BFS
    fringe.add(root);
    while (!fringe.isEmpty()) {
      ColumnPredicate current = fringe.remove();
      if (current instanceof ColumnPredicate.LogicColumnPredicate) {
        fringe.add(((ColumnPredicate.LogicColumnPredicate) current).lhs());
        fringe.add(((ColumnPredicate.LogicColumnPredicate) current).rhs());
      }

      callback.perform(current);
    }
  }

  @Test
  public void testAllShareParent() {
    assertTrue(determineShareParent(FIRST, NODES_IN_FIRST));
    assertTrue(determineShareParent(SECOND, NODES_IN_SECOND));
    assertTrue(determineShareParent(THIRD, NODES_IN_THIRD));

    assertFalse(determineShareParent(FIRST, NODES_IN_SECOND));
    assertFalse(determineShareParent(FIRST, NODES_IN_THIRD));
    assertFalse(determineShareParent(SECOND, NODES_IN_FIRST));
    assertFalse(determineShareParent(SECOND, NODES_IN_THIRD));
    assertFalse(determineShareParent(THIRD, NODES_IN_FIRST));
    assertFalse(determineShareParent(THIRD, NODES_IN_SECOND));
  }

  private boolean determineShareParent(ColumnPredicate parent, Iterable<ColumnPredicate> children) {
    for (ColumnPredicate child : children) {
      if (parent != determineRoot(child)) {
        return false;
      }
    }

    return true;
  }

  private static ColumnPredicate determineRoot(ColumnPredicate node) {
    ColumnPredicate current = node;
    while (current.hasParent()) {
      current = current.parent();
    }

    return current;
  }
}
