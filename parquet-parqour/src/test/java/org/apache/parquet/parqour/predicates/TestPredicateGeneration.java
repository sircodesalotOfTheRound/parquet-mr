package org.apache.parquet.parqour.predicates;

import org.apache.parquet.parqour.exceptions.ColumnPredicateBuilderException;
import org.apache.parquet.parqour.ingest.plan.predicates.ColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.builders.ColumnPredicateBuildable;
import org.apache.parquet.parqour.ingest.plan.predicates.builders.leaf.sys.*;
import org.apache.parquet.parqour.ingest.plan.predicates.builders.logic.AndColumnPredicateBuilder;
import org.apache.parquet.parqour.ingest.plan.predicates.builders.logic.OrColumnPredicateBuilder;
import org.apache.parquet.parqour.ingest.plan.predicates.leaf.sys.*;
import org.apache.parquet.parqour.ingest.plan.predicates.types.ColumnPredicateNodeCategory;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.parqour.testtools.TestTools;
import org.junit.Test;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by sircodesalot on 6/2/15.
 */
public class TestPredicateGeneration {
  private static final MessageType SINGLE_COLUMN_SCHEMA = new MessageType("single_column_schema",
    new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "numeric_column"));

  private static final ColumnDescriptor IDENTIFIER =
    SINGLE_COLUMN_SCHEMA.getColumnDescription(new String[] { "numeric_column" });
  private static final IngestTree INGEST_TREE = TestTools.generateIngestTreeFromSchema(SINGLE_COLUMN_SCHEMA);
  private static final Integer VALUE = new Random().nextInt(500);

  private static final ColumnPredicateBuildable lhs = new EqualsColumnPredicateBuilder(IDENTIFIER, VALUE);
  private static final ColumnPredicateBuildable rhs = new NotEqualsColumnPredicateBuilder(IDENTIFIER, VALUE);

  @Test
  public void testLeafPredicateGeneration() {
    buildAndCheck(new EqualsColumnPredicateBuilder(IDENTIFIER, VALUE), EqualsColumnPredicate.class);
    buildAndCheck(new NotEqualsColumnPredicateBuilder(IDENTIFIER, VALUE), NotEqualsColumnPredicate.class);
    buildAndCheck(new LessThanColumnPredicateBuilder(IDENTIFIER, VALUE), LessThanColumnPredicate.class);
    buildAndCheck(new LessThanOrEqualsColumnPredicateBuilder(IDENTIFIER, VALUE), LessThanOrEqualsColumnPredicate.class);
    buildAndCheck(new GreaterThanColumnPredicateBuilder(IDENTIFIER, VALUE), GreaterThanColumnPredicate.class);
    buildAndCheck(new GreaterThanOrEqualsColumnPredicateBuilder(IDENTIFIER, VALUE), GreaterThanOrEqualsColumnPredicate.class);
  }

  private <T extends ColumnPredicate.SystemDefinedPredicate> void buildAndCheck(ColumnPredicateBuildable buildable, Class<T> type) {
    T predicate = (T) buildable.build(null, INGEST_TREE);
    assertTrue(type.isAssignableFrom(predicate.getClass()));

    assertEquals(predicate.nodeCategory(), ColumnPredicateNodeCategory.SYSTEM_DEFINED_LEAF);
    assertEquals(predicate.column(),IDENTIFIER);
    assertEquals(predicate.value(), VALUE);
  }

  @Test
  public void testLogicalPredicateGeneration() {
    new AndColumnPredicateBuilder(lhs, rhs);
  }

  @Test(expected=ColumnPredicateBuilderException.class)
  public void testDisallowNullChildForAndPredicateBuilder() {
    new AndColumnPredicateBuilder(null, null);
  }

  @Test(expected=ColumnPredicateBuilderException.class)
  public void testDisallowNullChildForOrPredicateBuilder() {
    new OrColumnPredicateBuilder(null, null);
  }
}