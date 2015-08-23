package org.apache.parquet.parqour.query.expressions.predicates;

import org.apache.parquet.parqour.ingest.plan.predicates.traversal.EvaluationDifficulty;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.parqour.query.expressions.predicate.TextQueryTestablePredicateExpression;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
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

  @Test
  public void testDualColumnEvaluationDifficulty() {
    Map<PrimitiveType.PrimitiveTypeName, EvaluationDifficulty> difficultyForType =
      new HashMap<PrimitiveType.PrimitiveTypeName, EvaluationDifficulty>();

    // TODO: Uncomment when ingest nodes are available.
    difficultyForType.put(INT32, EvaluationDifficulty.EASY);
    difficultyForType.put(INT64, EvaluationDifficulty.EASY);
    //difficultyForType.put(INT96, EvaluationDifficulty.MEDIUM);
    //difficultyForType.put(FLOAT, EvaluationDifficulty.EASY);
    //difficultyForType.put(DOUBLE, EvaluationDifficulty.EASY);
    //difficultyForType.put(BOOLEAN, EvaluationDifficulty.EASY);
    difficultyForType.put(BINARY, EvaluationDifficulty.DIFFICULT);
    //difficultyForType.put(FIXED_LEN_BYTE_ARRAY, EvaluationDifficulty.MEDIUM);

    for (PrimitiveType.PrimitiveTypeName type : difficultyForType.keySet()) {
      for (String sign : new String[]{ "=", "!=", ">", ">=", "<=" }) {
        String expression = String.format("select * where lhs %s rhs", sign);
        assertEvaluationDifficultyIs(new MessageType("schema",
            new PrimitiveType(REQUIRED, type, "lhs"),
            new PrimitiveType(REQUIRED, type, "rhs")),
          expression, difficultyForType.get(type));
      }
    }
  }

  private void assertEvaluationDifficultyIs(String expression, EvaluationDifficulty difficulty) {
    TextQueryTestablePredicateExpression testableExpression = TestTools.simplifiedPredicateFromString(expression);
    EvaluationDifficulty fromExpression = testableExpression.evaluationDifficulty();

    assertEquals("Evaluation difficulties should be the same.", fromExpression, difficulty);
  }

  private void assertEvaluationDifficultyIs(MessageType schema, String expression, EvaluationDifficulty difficulty) {
    IngestTree tree = TestTools.generateIngestTreeFromSchema(schema);
    TextQueryTestablePredicateExpression testableExpression = TestTools.simplifiedPredicateFromString(expression);
    testableExpression.bindToTree(tree);

    EvaluationDifficulty fromExpression = testableExpression.evaluationDifficulty();
    assertEquals("Evaluation difficulties should be the same.", fromExpression, difficulty);
  }
}
