package org.apache.parquet.parqour.plan;

import org.apache.parquet.parqour.ingest.plan.analysis.PredicateAnalysis;
import org.apache.parquet.parqour.ingest.plan.evaluation.EvaluationPathAnalysis;
import org.apache.parquet.parqour.ingest.plan.evaluation.skipchain.SkipChain;
import org.apache.parquet.parqour.ingest.plan.evaluation.waypoints.PredicateTestWayPoint;
import org.apache.parquet.parqour.ingest.plan.evaluation.waypoints.WayPoint;
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

/**
 * Created by sircodesalot on 6/8/15.
 */
public class TestEvaluationPaths {
  private static final MessageType SINGLE_COLUMN_SCHEMA = new MessageType("single_column_schema",
    new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "numeric_column"));


  @Test
  public void testSinglePredicate() {
    Operators.Eq<Integer> equalsPredicate = eq(TestTools.NUMERIC_COLUMN, 0);

    IngestTree ingestTree = TestTools.generateIngestTreeFromSchema(SINGLE_COLUMN_SCHEMA, equalsPredicate);
    PredicateAnalysis analysis = new PredicateAnalysis(ingestTree);
    EvaluationPathAnalysis pathAnalysis = new EvaluationPathAnalysis(ingestTree, analysis);
    PredicateTestWayPoint path = pathAnalysis.path();

    assertEquals(path.successPath(), PredicateTestWayPoint.SUCCESS);
    assertEquals(path.failurePath(), PredicateTestWayPoint.FAILURE);

    assertEquals(0, chainLength(path.successSkipChain()));
    assertEquals(0, chainLength(path.failureSkipChain()));
  }

  @Test
  public void testAndPredicate() {
    FilterPredicate andPredicate = and(eq(TestTools.NUMERIC_COLUMN, 0), eq(TestTools.NUMERIC_COLUMN, 0));
    IngestTree ingestTree = TestTools.generateIngestTreeFromSchema(SINGLE_COLUMN_SCHEMA, andPredicate);
    PredicateAnalysis analysis = new PredicateAnalysis(ingestTree);
    EvaluationPathAnalysis pathAnalysis = new EvaluationPathAnalysis(ingestTree, analysis);
    PredicateTestWayPoint path = pathAnalysis.path();

    assertEquals(path.successPath().successPath(), PredicateTestWayPoint.SUCCESS);
    assertEquals(path.failurePath(), PredicateTestWayPoint.FAILURE);

    // Since this is an AND, you can only skip by failure.
    assertEquals(0, chainLength(path.successSkipChain()));
    assertEquals(1, chainLength(path.failureSkipChain()));

    assertEquals(0, chainLength(path.successPath().successSkipChain()));
    assertEquals(0, chainLength(path.successPath().failureSkipChain()));
  }

  @Test
  public void testOrPredicate() {
    FilterPredicate andPredicate = or(eq(TestTools.NUMERIC_COLUMN, 0), eq(TestTools.NUMERIC_COLUMN, 0));
    IngestTree ingestTree = TestTools.generateIngestTreeFromSchema(SINGLE_COLUMN_SCHEMA, andPredicate);
    PredicateAnalysis analysis = new PredicateAnalysis(ingestTree);
    EvaluationPathAnalysis pathAnalysis = new EvaluationPathAnalysis(ingestTree, analysis);
    PredicateTestWayPoint path = pathAnalysis.path();

    assertEquals(path.successPath(), PredicateTestWayPoint.SUCCESS);
    assertEquals(path.failurePath().failurePath(), PredicateTestWayPoint.FAILURE);

    // If the first item is read successfully, then we can skip reading the second.
    assertEquals(1, chainLength(path.successSkipChain()));
    assertEquals(0, chainLength(path.failurePath().failureSkipChain()));
  }

  @Test
  public void testAndOrPredicate() {
    // Note that the traversal always follows the easiest path first (in this case, it goes the tree
    // with the shortest length, which is the 'right' side first, NOT the 'left'!)
    FilterPredicate andPredicate  =
      and(
        or(eq(TestTools.NUMERIC_COLUMN, 0), eq(TestTools.NUMERIC_COLUMN, 0)),
        eq(TestTools.NUMERIC_COLUMN, 0));

    IngestTree ingestTree = TestTools.generateIngestTreeFromSchema(SINGLE_COLUMN_SCHEMA, andPredicate);
    PredicateAnalysis analysis = new PredicateAnalysis(ingestTree);
    EvaluationPathAnalysis pathAnalysis = new EvaluationPathAnalysis(ingestTree, analysis);
    PredicateTestWayPoint path = pathAnalysis.path();

    // Remember that the path will be optimized to start on the right side first, not the left!
    // So here we're moving from right to left.

    // eq.true -> AND(OR(lhs.true)) -> Success
    assertEquals(path.successPath().successPath(), PredicateTestWayPoint.SUCCESS);

    // eq.true -> AND(OR(lhs.false -> rhs.true)) -> Success
    assertEquals(path.successPath().failurePath().successPath(), PredicateTestWayPoint.SUCCESS);

    // eq.false -> Failure
    assertEquals(path.failurePath(), PredicateTestWayPoint.FAILURE);

    // eq.true -> AND(OR(lhs.false -> rhs.false)) -> Failure
    assertEquals(path.successPath().failurePath().failurePath(), PredicateTestWayPoint.FAILURE);

    // Compute skip sets
    assertEquals(0, chainLength(path.successSkipChain()));
    assertEquals(2, chainLength(path.failureSkipChain()));
    assertEquals(1, chainLength(path.successPath().successSkipChain()));
  }

  @Test
  public void testOrAndPredicate() {
    // Note that the traversal always follows the easiest path first (in this case, it goes the tree
    // with the shortest length, which is the 'right' side first, NOT the 'left'!)
    FilterPredicate orPredicate  =
      or(
        and(eq(TestTools.NUMERIC_COLUMN, 0), eq(TestTools.NUMERIC_COLUMN, 0)),
        eq(TestTools.NUMERIC_COLUMN, 0));

    IngestTree ingestTree = TestTools.generateIngestTreeFromSchema(SINGLE_COLUMN_SCHEMA, orPredicate);
    PredicateAnalysis analysis = new PredicateAnalysis(ingestTree);
    EvaluationPathAnalysis pathAnalysis = new EvaluationPathAnalysis(ingestTree, analysis);
    PredicateTestWayPoint path = pathAnalysis.path();

    // Remember that the path will be optimized to start on the right side first, not the left!
    // So here we're moving from right to left.

    // EQ -> Success (passes through or by short-curcuit onto success).
    assertEquals(path.successPath(), PredicateTestWayPoint.SUCCESS);

    // OR (EQ -> AND(lhs -> rhs)) -> Success
    assertEquals(path.failurePath().successPath().successPath(), PredicateTestWayPoint.SUCCESS);

    // OR (EQ -> AND(lhs)) -> Failure // Short circuit to Failure
    assertEquals(path.failurePath().failurePath(), PredicateTestWayPoint.FAILURE);

    // OR(EQ -> AND(lhs -> rhs)) -> Failure
    assertEquals(path.failurePath().successPath().failurePath(), PredicateTestWayPoint.FAILURE);
  }

  private int chainLength(SkipChain chain) {
    int count = 0;
    for (WayPoint current = chain.path(); current != null; current = current.successPath()) {
      count++;
    }

    return count;
  }
}
