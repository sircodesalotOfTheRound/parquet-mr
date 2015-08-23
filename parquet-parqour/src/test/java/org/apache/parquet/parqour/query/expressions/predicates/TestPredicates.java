package org.apache.parquet.parqour.query.expressions.predicates;

import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.predicate.TextQueryTestablePredicateExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryTreeRootExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryWhereExpression;
import org.apache.parquet.parqour.query.visitor.InfixPredicateCollectingVisitor;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 7/12/15.
 */
public class TestPredicates {
  private static final MessageType SCHEMA = new MessageType("schema",
    new PrimitiveType(REQUIRED, INT32, "first"),
    new GroupType(REQUIRED, "grouping",
      new PrimitiveType(REQUIRED, INT32, "second"))
  );

  private static final IngestTree SCHEMA_INGEST_TREE = TestTools.generateIngestTreeFromSchema(SCHEMA);

  @Test
  @Deprecated
  public void testLevelOneBinaryPredicate() {
    Operators.Eq predicate = generateFilterApiFromExpression("select * where first = 10");
    assertColumnPathsAreEqual(predicate.getColumn(), "first");
    assertEquals(predicate.getValue(), 10);
  }

  @Test
  @Deprecated
  public void testNestedBinaryPredicateApplication() {
    Operators.Eq predicate = generateFilterApiFromExpression("select * where grouping.second = 42");
    assertColumnPathsAreEqual(predicate.getColumn(), "grouping.second");
    assertEquals(predicate.getValue(), 42);
  }


  @Deprecated
  private <T extends FilterPredicate> T generateFilterApiFromExpression(String expression) {
    TextQueryTreeRootExpression rootExpression = TextQueryTreeRootExpression.fromString(expression);
    TextQueryWhereExpression whereExpression = rootExpression.asSelectStatement().where();

    InfixPredicateCollectingVisitor visitor = new InfixPredicateCollectingVisitor(SCHEMA_INGEST_TREE, whereExpression);
    return (T) visitor.predicate();
  }

  private void assertColumnPathsAreEqual(Operators.Column expected, String actual) {
    String expectedAsString = expected.getColumnPath().toDotString();
    assertEquals(expectedAsString, actual);
  }
}
