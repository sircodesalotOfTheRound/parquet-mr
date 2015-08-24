package org.apache.parquet.parqour.query.expressions.predicates;

import org.apache.parquet.parqour.ingest.plan.predicates.traversal.EvaluationDifficulty;
import org.apache.parquet.parqour.ingest.plan.predicates.traversal.TraversalPreference;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.parqour.query.expressions.predicate.TextQueryTestablePredicateExpression;
import org.apache.parquet.parqour.query.expressions.predicate.logical.TextQueryLogicalAndExpression;
import org.apache.parquet.parqour.query.expressions.predicate.logical.TextQueryLogicalExpression;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import java.util.*;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 8/23/15.
 */
public class TestEvaluationDifficulty {
  private static final Map<PrimitiveType.PrimitiveTypeName, EvaluationDifficulty> DIFFICULTY_BY_TYPE = genereateDifficultyByTypeMap();
  private static final List<PrimitiveType.PrimitiveTypeName> TYPE_LIST = generateTypeList(DIFFICULTY_BY_TYPE);

  private static Map<PrimitiveType.PrimitiveTypeName, EvaluationDifficulty> genereateDifficultyByTypeMap() {
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

    return difficultyForType;
  }

  private static List<PrimitiveType.PrimitiveTypeName> generateTypeList(Map<PrimitiveType.PrimitiveTypeName, EvaluationDifficulty> difficultyByType) {
    List<PrimitiveType.PrimitiveTypeName> typeList = new ArrayList<PrimitiveType.PrimitiveTypeName>();
    for (PrimitiveType.PrimitiveTypeName type : difficultyByType.keySet()) {
      typeList.add(type);
    }

    return typeList;
  }

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
    for (PrimitiveType.PrimitiveTypeName type : DIFFICULTY_BY_TYPE.keySet()) {
      for (String sign : new String[]{"=", "!=", ">", ">=", "<="}) {
        String expression = String.format("select * where lhs %s rhs", sign);
        assertEvaluationDifficultyIs(new MessageType("schema",
          new PrimitiveType(REQUIRED, type, "lhs"),
          new PrimitiveType(REQUIRED, type, "rhs")),
        expression, DIFFICULTY_BY_TYPE.get(type));
      }
    }
  }

  @Test
  public void testLogicalEvaluationPreference() {
    for (PrimitiveType.PrimitiveTypeName lhsType : TYPE_LIST) {
      for (PrimitiveType.PrimitiveTypeName rhsType : TYPE_LIST) {
        EvaluationDifficulty lhsEvaluationDifficulty = DIFFICULTY_BY_TYPE.get(lhsType);
        EvaluationDifficulty rhsEvaluationDifficulty = DIFFICULTY_BY_TYPE.get(rhsType);
        TraversalPreference preference = lhsEvaluationDifficulty.compareTo(rhsEvaluationDifficulty) <= 0 ?
          TraversalPreference.LEFT_NODE : TraversalPreference.RIGHT_NODE;

        for (String logicalOperator : new String[]{ "AND", "and", "OR", "or" }) {
          for (String sign : new String[]{"=", "!=", ">", ">=", "<="}) {
            String expression = String.format("select * where (lhs %s lhs) %s (rhs %s rhs)", sign, logicalOperator, sign);
            assertTraversalPreferenceIs(new MessageType("schema",
                new PrimitiveType(REQUIRED, lhsType, "lhs"),
                new PrimitiveType(REQUIRED, rhsType, "rhs")),
              expression, preference);
          }
        }
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

    EvaluationDifficulty difficultyFromExpression = testableExpression.evaluationDifficulty();
    assertEquals("Evaluation difficulties should be the same.", difficulty, difficultyFromExpression);
  }

  private void assertTraversalPreferenceIs(MessageType schema, String expression, TraversalPreference preference) {
    IngestTree tree = TestTools.generateIngestTreeFromSchema(schema);
    TextQueryLogicalExpression logicalExpression = TestTools.simplifiedPredicateFromString(expression);
    logicalExpression.bindToTree(tree);

    TraversalPreference traversalPreferencefromExpression = logicalExpression.traversalPreference();
    String errorMessage = String.format("Given expression \"%s\" (lhs : %s, rhs : %s) expected preference: %s",
      expression,
      schema.getType("lhs").asPrimitiveType().getPrimitiveTypeName(),
      schema.getType("rhs").asPrimitiveType().getPrimitiveTypeName(),
      preference);

    assertEquals(errorMessage, preference, traversalPreferencefromExpression);
  }
}
