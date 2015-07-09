package org.apache.parquet.parqour.analysis;

import org.apache.parquet.parqour.ingest.plan.analysis.PredicateAnalysis;
import org.apache.parquet.parqour.ingest.plan.predicates.traversal.TraversalPreference;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.parqour.testtools.TestTools;
import org.junit.Test;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import static org.junit.Assert.assertEquals;
import static org.apache.parquet.filter2.predicate.FilterApi.*;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;


/**
 * Created by sircodesalot on 6/8/15.
 */
public class TestTraversalInfo {
  private static final MessageType SCHEMA = new MessageType("single_column_schema",
    new PrimitiveType(Type.Repetition.REQUIRED, BINARY, "binary_column"),
    new PrimitiveType(Type.Repetition.REQUIRED, INT32, "numeric_column"));

  private static final IngestTree INGEST_TREE = TestTools.generateIngestTreeFromSchema(SCHEMA);

  @Test
  public void testEndNodeTraversalPreference() {
    assertEquals(captureTraversalPreference(eq(TestTools.NUMERIC_COLUMN, 0)), TraversalPreference.THIS_NODE);
    assertEquals(captureTraversalPreference(notEq(TestTools.NUMERIC_COLUMN, 0)), TraversalPreference.THIS_NODE);
    assertEquals(captureTraversalPreference(lt(TestTools.NUMERIC_COLUMN, 0)), TraversalPreference.THIS_NODE);
    assertEquals(captureTraversalPreference(gt(TestTools.NUMERIC_COLUMN, 0)), TraversalPreference.THIS_NODE);
    assertEquals(captureTraversalPreference(ltEq(TestTools.NUMERIC_COLUMN, 0)), TraversalPreference.THIS_NODE);
    assertEquals(captureTraversalPreference(gtEq(TestTools.NUMERIC_COLUMN, 0)), TraversalPreference.THIS_NODE);

    // Should always choose a numeric comparison path rather than a binary comparison path.
    FilterPredicate numericVsBinary = and(eq(TestTools.BINARY_COLUMN, null), eq(TestTools.NUMERIC_COLUMN, null));
    assert (captureTraversalPreference(numericVsBinary) == TraversalPreference.RIGHT_NODE);

    // Otherwise choose the path of least distance.
    FilterPredicate shortPathVsLongPath = and(eq(TestTools.NUMERIC_COLUMN, null),
      and(eq(TestTools.NUMERIC_COLUMN, null), eq(TestTools.NUMERIC_COLUMN, 0)));
    assert (captureTraversalPreference(shortPathVsLongPath) == TraversalPreference.LEFT_NODE);

    // Else choose the path of greatest homogenity (continuous ANDs or continuous ORs).
    FilterPredicate greatestHomogenity = and(
      or(eq(TestTools.NUMERIC_COLUMN, null), eq(TestTools.NUMERIC_COLUMN, 0)),
      and(eq(TestTools.NUMERIC_COLUMN, null), eq(TestTools.NUMERIC_COLUMN, 0)));
    assert (captureTraversalPreference(greatestHomogenity) == TraversalPreference.RIGHT_NODE);

    // Else choose the left node.
    FilterPredicate equivalentPaths = and(eq(TestTools.NUMERIC_COLUMN, null), eq(TestTools.NUMERIC_COLUMN, null));
    assert (captureTraversalPreference(equivalentPaths) == TraversalPreference.LEFT_NODE);
  }

  private TraversalPreference captureTraversalPreference(FilterPredicate predicate) {
    IngestTree ingestTree = TestTools.generateIngestTreeFromSchema(SCHEMA, predicate);
    PredicateAnalysis analysis = new PredicateAnalysis(ingestTree);
    return analysis.predicateTree().traversalInfo().traversalPreference();
  }

  @Test
  public void testEndNodeDepth() {
    assertEquals(computeNodeDepth(eq(TestTools.NUMERIC_COLUMN, 0)), 1);
    assertEquals(computeNodeDepth(notEq(TestTools.NUMERIC_COLUMN, 0)), 1);
    assertEquals(computeNodeDepth(lt(TestTools.NUMERIC_COLUMN, 0)), 1);
    assertEquals(computeNodeDepth(gt(TestTools.NUMERIC_COLUMN, 0)), 1);
    assertEquals(computeNodeDepth(ltEq(TestTools.NUMERIC_COLUMN, 0)), 1);
    assertEquals(computeNodeDepth(gtEq(TestTools.NUMERIC_COLUMN, 0)), 1);


    Operators.Eq<Integer> lhs = eq(TestTools.NUMERIC_COLUMN, 0);
    Operators.Eq<Integer> rhs = eq(TestTools.NUMERIC_COLUMN, 0);
    FilterPredicate levelTwoAnd = and(lhs, rhs);
    FilterPredicate levelThreeOr = or(levelTwoAnd, eq(TestTools.NUMERIC_COLUMN, 0));
    FilterPredicate levelFourAnd = and(levelThreeOr, eq(TestTools.NUMERIC_COLUMN, 0));

    assertEquals(computeNodeDepth(levelTwoAnd), 2);
    assertEquals(computeNodeDepth(levelThreeOr), 3);
    assertEquals(computeNodeDepth(levelFourAnd), 4);
  }

  private int computeNodeDepth(FilterPredicate predicate) {
    IngestTree ingestTree = TestTools.generateIngestTreeFromSchema(SCHEMA, predicate);
    PredicateAnalysis analysis = new PredicateAnalysis(ingestTree);
    return analysis.predicateTree().traversalInfo().depth();
  }

  @Test
  public void testPreferredPath() {
  }
}
