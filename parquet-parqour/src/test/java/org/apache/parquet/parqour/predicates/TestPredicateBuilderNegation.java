package org.apache.parquet.parqour.predicates;

import org.apache.parquet.parqour.ingest.plan.predicates.ColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.builders.ColumnPredicateBuildable;
import org.apache.parquet.parqour.ingest.plan.predicates.builders.leaf.sys.*;
import org.apache.parquet.parqour.ingest.plan.predicates.builders.logic.AndColumnPredicateBuilder;
import org.apache.parquet.parqour.ingest.plan.predicates.builders.logic.OrColumnPredicateBuilder;
import org.apache.parquet.parqour.ingest.plan.predicates.leaf.sys.*;
import org.apache.parquet.parqour.ingest.plan.predicates.logic.AndColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.logic.OrColumnPredicate;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.parqour.testtools.TestTools;
import org.junit.Test;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import static org.junit.Assert.assertEquals;

/**
 * Created by sircodesalot on 6/2/15.
 */
public class TestPredicateBuilderNegation {
  private static final MessageType SINGLE_COLUMN_SCHEMA = new MessageType("single_column_schema",
    new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "numeric_column"));

  private static final ColumnDescriptor COLUMN = TestTools.getColumnFromSchema(SINGLE_COLUMN_SCHEMA, "numeric_column");
  private static final IngestTree INGEST_TREE = TestTools.generateIngestTreeFromSchema(SINGLE_COLUMN_SCHEMA);
  private static final Integer VALUE = 10;

  private static final ColumnPredicateBuildable LHS = new EqualsColumnPredicateBuilder(COLUMN, VALUE);
  private static final ColumnPredicateBuildable RHS = new NotEqualsColumnPredicateBuilder(COLUMN, VALUE);

  @Test
  public void testNegatePredicate() {
    testBeforeAndAfterNegation(new EqualsColumnPredicateBuilder(COLUMN, VALUE),
      EqualsColumnPredicate.class, NotEqualsColumnPredicate.class);

    testBeforeAndAfterNegation(new NotEqualsColumnPredicateBuilder(COLUMN, VALUE),
      NotEqualsColumnPredicate.class, EqualsColumnPredicate.class);

    testBeforeAndAfterNegation(new LessThanColumnPredicateBuilder(COLUMN, VALUE),
      LessThanColumnPredicate.class, GreaterThanOrEqualsColumnPredicate.class);

    testBeforeAndAfterNegation(new LessThanOrEqualsColumnPredicateBuilder(COLUMN, VALUE),
      LessThanOrEqualsColumnPredicate.class, GreaterThanColumnPredicate.class);

    testBeforeAndAfterNegation(new GreaterThanColumnPredicateBuilder(COLUMN, VALUE),
      GreaterThanColumnPredicate.class, LessThanOrEqualsColumnPredicate.class);

    testBeforeAndAfterNegation(new GreaterThanOrEqualsColumnPredicateBuilder(COLUMN, VALUE),
      GreaterThanOrEqualsColumnPredicate.class, LessThanColumnPredicate.class);

    testBeforeAndAfterNegation(new AndColumnPredicateBuilder(LHS, RHS),
      AndColumnPredicate.class, OrColumnPredicate.class);

    testBeforeAndAfterNegation(new OrColumnPredicateBuilder(LHS, RHS),
      OrColumnPredicate.class, AndColumnPredicate.class);

    testBeforeAndAfterNegation(new OrColumnPredicateBuilder(LHS, RHS),
      OrColumnPredicate.class, AndColumnPredicate.class);
  }

  private <T extends ColumnPredicate, U extends ColumnPredicate>
    void testBeforeAndAfterNegation(ColumnPredicateBuildable buildable, Class<T> positive, Class<U> negative) {

    ColumnPredicate before = buildable.build(null, INGEST_TREE);
    buildable.negate();
    ColumnPredicate after = buildable.build(null, INGEST_TREE);

    assertEquals(positive, before.getClass());
    assertEquals(negative, after.getClass());
  }

  @Test
  public void testDemorganization() {
    testDeMorganzed(
      new AndColumnPredicateBuilder(
        new LessThanColumnPredicateBuilder(COLUMN, VALUE),
        new GreaterThanColumnPredicateBuilder(COLUMN, VALUE)),
      GreaterThanOrEqualsColumnPredicate.class,
      LessThanOrEqualsColumnPredicate.class);

    testDeMorganzed(
      new OrColumnPredicateBuilder(
        new EqualsColumnPredicateBuilder(COLUMN, VALUE),
        new NotEqualsColumnPredicateBuilder(COLUMN, VALUE)),
      NotEqualsColumnPredicate.class,
      EqualsColumnPredicate.class);
  }

  private <T extends ColumnPredicate, U extends ColumnPredicate>
    void testDeMorganzed(ColumnPredicateBuildable.LogicColumnPredicateBuilder builder,
                         Class<T> lhsNegative, Class<U> rhsNegative) {
    builder.negate();
    ColumnPredicate.LogicColumnPredicate predicate = (ColumnPredicate.LogicColumnPredicate)builder.build(null, INGEST_TREE);
    ColumnPredicate lhs = predicate.lhs();
    ColumnPredicate rhs = predicate.rhs();

    assertEquals(lhsNegative,lhs.getClass());
    assertEquals(rhsNegative,rhs.getClass());
  }
}
