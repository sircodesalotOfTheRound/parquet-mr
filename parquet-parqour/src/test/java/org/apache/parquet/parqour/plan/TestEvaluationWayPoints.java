package org.apache.parquet.parqour.plan;

import org.apache.parquet.parqour.ingest.plan.evaluation.skipchain.SkipChain;
import org.apache.parquet.parqour.ingest.plan.evaluation.waypoints.PredicateTestWayPoint;
import org.apache.parquet.parqour.ingest.plan.evaluation.waypoints.SkipChainWayPoint;
import org.apache.parquet.parqour.ingest.plan.evaluation.waypoints.WayPoint;
import org.apache.parquet.parqour.ingest.plan.evaluation.waypoints.WayPointCategory;
import org.apache.parquet.parqour.ingest.plan.predicates.ColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.leaf.sys.EqualsColumnPredicate;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.parqour.testtools.TestTools;
import org.junit.Test;
import org.apache.parquet.column.ColumnDescriptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by sircodesalot on 6/8/15.
 */
public class TestEvaluationWayPoints {
  private static final ColumnDescriptor NUMERIC_COLUMN = TestTools.getColumnFromSchema(TestTools.SINGLE_COLUMN_SCHEMA, "numeric_column");
  private static final IngestTree INGEST_TREE = TestTools.generateIngestTreeFromSchema(TestTools.SINGLE_COLUMN_SCHEMA);
  private static final EqualsColumnPredicate EQUALS_PREDICATE = new EqualsColumnPredicate(INGEST_TREE, null, NUMERIC_COLUMN, 0);

  @Test
  public void testTautologicalWayPoints() {
    PredicateTestWayPoint success = PredicateTestWayPoint.SUCCESS;
    PredicateTestWayPoint failure = PredicateTestWayPoint.FAILURE;

    assertEquals (success, success.successPath());
    assertEquals (success, success.failurePath());

    assertEquals(failure, failure.successPath());
    assertEquals(failure, failure.failurePath());
  }

  @Test
  public void testEvaluationPaths() {
    PredicateTestWayPoint path =
      new PredicateTestWayPoint(INGEST_TREE, EQUALS_PREDICATE,
        new PredicateTestWayPoint(INGEST_TREE, EQUALS_PREDICATE, PredicateTestWayPoint.SUCCESS, PredicateTestWayPoint.FAILURE),
        new PredicateTestWayPoint(INGEST_TREE, EQUALS_PREDICATE, PredicateTestWayPoint.SUCCESS, PredicateTestWayPoint.FAILURE)
      );

    assertEquals(path.successPath().successPath(), PredicateTestWayPoint.SUCCESS);
    assertEquals(path.failurePath().successPath(), PredicateTestWayPoint.SUCCESS);

    assertEquals(path.successPath().failurePath(), PredicateTestWayPoint.FAILURE);
    assertEquals(path.failurePath().failurePath(), PredicateTestWayPoint.FAILURE);
  }

  @Test
  public void testSkipChainWayPointReassignment() {
    SkipChainWayPoint first = new SkipChainWayPoint(INGEST_TREE, EQUALS_PREDICATE);
    SkipChainWayPoint second = new SkipChainWayPoint(INGEST_TREE, EQUALS_PREDICATE);

    assertEquals(first.category(), WayPointCategory.READ);
    assertEquals(second.category(), WayPointCategory.READ);

    assertNull(first.successPath());
    assertNull(first.failurePath());

    first.setNext(second);

    assertEquals(first.successPath(), second);
    assertEquals(first.failurePath(), second);

    assertNull(second.successPath());
    assertNull(second.failurePath());

    second.setNext(first);
    assertEquals(second.successPath(), first);
    assertEquals(second.failurePath(), first);
  }

  @Test
  public void testSkipChain() {
    SkipChain emptyChain = new SkipChain();
    SkipChain oneNodeChain = new SkipChain(INGEST_TREE, new ColumnPredicate.LeafColumnPredicate[] {
      EQUALS_PREDICATE,
    });
    SkipChain twoNodeChain = new SkipChain(INGEST_TREE, new ColumnPredicate.LeafColumnPredicate[] {
      EQUALS_PREDICATE,
      EQUALS_PREDICATE
    });
    SkipChain threeNodeChain = new SkipChain(INGEST_TREE, new ColumnPredicate.LeafColumnPredicate[] {
      EQUALS_PREDICATE,
      EQUALS_PREDICATE,
      EQUALS_PREDICATE
    });


    assertNull(emptyChain.path());

    assertEquals(0, chainLength(emptyChain));
    assertEquals(3, chainLength(threeNodeChain));

    SkipChain wasEmptyNowThreeChain = emptyChain.append(threeNodeChain);
    assertEquals(3, chainLength(wasEmptyNowThreeChain));

    SkipChain emptyAgain = wasEmptyNowThreeChain.reset();
    assertEquals(0, chainLength(emptyAgain));

    SkipChain stillThreeNodes = threeNodeChain.append(emptyChain);
    assertEquals(3, chainLength(stillThreeNodes));

    SkipChain resetToThreeNodes = stillThreeNodes.reset();
    assertEquals(3, chainLength(resetToThreeNodes));

    SkipChain fiveNodeChain = threeNodeChain.append(twoNodeChain);
    assertEquals(5, chainLength(fiveNodeChain));

    SkipChain stillFiveNodeChain = emptyChain.append(fiveNodeChain);
    assertEquals(5, chainLength(stillFiveNodeChain));

    resetChains(emptyAgain, oneNodeChain, twoNodeChain, threeNodeChain);

    assertEquals(0, chainLength(emptyChain));
    assertEquals(1, chainLength(oneNodeChain));
    assertEquals(2, chainLength(twoNodeChain));
    assertEquals(3, chainLength(threeNodeChain));

    resetChains(emptyAgain, oneNodeChain, twoNodeChain, threeNodeChain);
    SkipChain sixLengthChain = emptyChain
      .append(oneNodeChain)
      .append(twoNodeChain)
      .append(threeNodeChain);

    assertEquals(6, chainLength(sixLengthChain));

    resetChains(emptyAgain, oneNodeChain, twoNodeChain, threeNodeChain);
    sixLengthChain = threeNodeChain
      .append(twoNodeChain)
      .append(oneNodeChain)
      .append(emptyChain);

    assertEquals(6, chainLength(sixLengthChain));

    resetChains(emptyAgain, oneNodeChain, twoNodeChain, threeNodeChain);
    sixLengthChain = twoNodeChain
      .append(threeNodeChain)
      .append(emptyChain)
      .append(oneNodeChain);

    assertEquals(6, chainLength(sixLengthChain));

    resetChains(emptyAgain, oneNodeChain, twoNodeChain, threeNodeChain);
    sixLengthChain = threeNodeChain
      .append(oneNodeChain)
      .append(emptyChain)
      .append(twoNodeChain);

    assertEquals(6, chainLength(sixLengthChain));

    resetChains(emptyAgain, oneNodeChain, twoNodeChain, threeNodeChain);
    sixLengthChain = oneNodeChain
      .append(emptyChain)
      .append(twoNodeChain)
      .append(threeNodeChain);

    assertEquals(6, chainLength(sixLengthChain));
  }

  private int chainLength(SkipChain chain) {
    int count = 0;
    for (WayPoint current = chain.path(); current != null; current = current.successPath()) {
      count++;
    }

    return count;
  }

  private void resetChains(SkipChain ... chains) {
    for (SkipChain chain : chains) {
      chain.reset();
    }
  }


}
