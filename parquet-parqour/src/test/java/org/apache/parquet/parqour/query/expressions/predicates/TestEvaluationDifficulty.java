package org.apache.parquet.parqour.query.expressions.predicates;

import org.apache.parquet.parqour.ingest.plan.predicates.traversal.EvaluationDifficulty;
import org.apache.parquet.parqour.query.expressions.predicate.TextQueryTestablePredicateExpression;
import org.apache.parquet.parqour.testtools.TestTools;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 8/23/15.
 */
public class TestEvaluationDifficulty {
  @Test
  public void testConstantValuePredicateDifficulty() {
    assertEvaluationDifficultyIs("select * where true", EvaluationDifficulty.EASY);
    assertEvaluationDifficultyIs("select * where false", EvaluationDifficulty.EASY);
    assertEvaluationDifficultyIs("select * where not false", EvaluationDifficulty.EASY);
    assertEvaluationDifficultyIs("select * where not true", EvaluationDifficulty.EASY);
    assertEvaluationDifficultyIs("select * where (not (true))", EvaluationDifficulty.EASY);
    assertEvaluationDifficultyIs("select * where (not (false))", EvaluationDifficulty.EASY);

    assertEvaluationDifficultyIs("select * where udf('complex')", EvaluationDifficulty.DIFFICULT);
  }

  public void assertEvaluationDifficultyIs(String expression, EvaluationDifficulty difficulty) {
    TextQueryTestablePredicateExpression testableExpression =  TestTools.simplifiedPredicateFromString(expression);
    EvaluationDifficulty fromExpression = testableExpression.evaluationDifficulty();

    assertEquals("Evaluation difficulties should be the same.", fromExpression, difficulty);
  }
}
