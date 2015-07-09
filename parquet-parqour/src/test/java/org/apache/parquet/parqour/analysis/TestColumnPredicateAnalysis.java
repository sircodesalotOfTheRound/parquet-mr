package org.apache.parquet.parqour.analysis;
import org.apache.parquet.parqour.ingest.plan.analysis.PredicateAnalysis;
import org.apache.parquet.parqour.ingest.plan.predicates.ColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.leaf.sys.*;
import org.apache.parquet.parqour.ingest.plan.predicates.leaf.udf.NegatedUserDefinedColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.leaf.udf.UserDefinedColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.logic.AndColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.logic.OrColumnPredicate;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.parqour.testtools.TestTools;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.filter2.predicate.*;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Test;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.parquet.filter2.predicate.FilterApi.*;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by sircodesalot on 6/2/15.
 */
public class TestColumnPredicateAnalysis {
  private static final MessageType SINGLE_COLUMN_SCHEMA = new MessageType("single_column_schema",
    new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "numeric_column"));

  private static final MessageType FIVE_COLUMN_SCHEMA = new MessageType("five_column_schema",
    new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "first"),
    new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "second"),
    new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "third"),
    new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "fourth"),
    new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "fifth")
  );

  private static final IngestTree SINGLE_COLUMN_SCHEMA_INGEST_TREE = TestTools.generateIngestTreeFromSchema(SINGLE_COLUMN_SCHEMA);
  private static final IngestTree FIVE_COLUMN_SCHEMA_INGEST_TREE = TestTools.generateIngestTreeFromSchema(FIVE_COLUMN_SCHEMA);

  private static final Operators.IntColumn NUMERIC_COLUMN = intColumn("numeric_column");
  private static final Integer TEN = 10;

  public static class MyUserDefinedPredicate extends UserDefinedPredicate<Integer> implements Serializable {
    @Override
    public boolean keep(Integer value) {
      return false;
    }

    @Override
    public boolean canDrop(Statistics<Integer> statistics) {
      return false;
    }

    @Override
    public boolean inverseCanDrop(Statistics<Integer> statistics) {
      return false;
    }
  }

  @Test
  public void testEqualsPredicateTreeGeneration() {
    assertIsa(eq(NUMERIC_COLUMN, TEN), EqualsColumnPredicate.class);
    assertIsa(notEq(NUMERIC_COLUMN, TEN), NotEqualsColumnPredicate.class);
    assertIsa(lt(NUMERIC_COLUMN, TEN), LessThanColumnPredicate.class);
    assertIsa(ltEq(NUMERIC_COLUMN, TEN), LessThanOrEqualsColumnPredicate.class);
    assertIsa(gt(NUMERIC_COLUMN, TEN), GreaterThanColumnPredicate.class);
    assertIsa(gtEq(NUMERIC_COLUMN, TEN), GreaterThanOrEqualsColumnPredicate.class);

    assertIsa(userDefined(NUMERIC_COLUMN, MyUserDefinedPredicate.class), UserDefinedColumnPredicate.class);
    assertIsa(userDefined(NUMERIC_COLUMN, new MyUserDefinedPredicate()), UserDefinedColumnPredicate.class);

    assertIsa(and(gt(NUMERIC_COLUMN, TEN), lt(NUMERIC_COLUMN, TEN)), AndColumnPredicate.class);
    assertIsa(or(gt(NUMERIC_COLUMN, TEN), lt(NUMERIC_COLUMN, TEN)), OrColumnPredicate.class);
  }

  @Test
  public void testNegation() {
    assertIsa(not(eq(NUMERIC_COLUMN, TEN)), NotEqualsColumnPredicate.class);
    assertIsa(not(notEq(NUMERIC_COLUMN, TEN)), EqualsColumnPredicate.class);
    assertIsa(not(lt(NUMERIC_COLUMN, TEN)), GreaterThanOrEqualsColumnPredicate.class);
    assertIsa(not(ltEq(NUMERIC_COLUMN, TEN)), GreaterThanColumnPredicate.class);
    assertIsa(not(gt(NUMERIC_COLUMN, TEN)), LessThanOrEqualsColumnPredicate.class);
    assertIsa(not(gtEq(NUMERIC_COLUMN, TEN)), LessThanColumnPredicate.class);

    assertIsa(not(userDefined(NUMERIC_COLUMN, MyUserDefinedPredicate.class)), NegatedUserDefinedColumnPredicate.class);
    assertIsa(not(userDefined(NUMERIC_COLUMN, new MyUserDefinedPredicate())), NegatedUserDefinedColumnPredicate.class);

    assertIsa(not(and(gt(NUMERIC_COLUMN, TEN), lt(NUMERIC_COLUMN, TEN))), OrColumnPredicate.class);
    assertIsa(not(or(gt(NUMERIC_COLUMN, TEN), lt(NUMERIC_COLUMN, TEN))), AndColumnPredicate.class);
  }

  private <T> void assertIsa(FilterPredicate predicate, Class<T> type) {
    IngestTree ingestTree = TestTools.generateIngestTreeFromSchema(SINGLE_COLUMN_SCHEMA, predicate);
    PredicateAnalysis analysis = new PredicateAnalysis(ingestTree);
    assertEquals(analysis.predicateTree().getClass(), type);
  }

  @Test
  public void testGatherLeaves() {
    assertTrue(leavesAreAll(and(gt(NUMERIC_COLUMN, TEN), lt(NUMERIC_COLUMN, TEN)),
      GreaterThanColumnPredicate.class, LessThanColumnPredicate.class));

    assertTrue(leavesAreAll(or(eq(NUMERIC_COLUMN, TEN), notEq(NUMERIC_COLUMN, TEN)),
      EqualsColumnPredicate.class, NotEqualsColumnPredicate.class));

    assertFalse(leavesAreAll(or(eq(NUMERIC_COLUMN, TEN), notEq(NUMERIC_COLUMN, TEN)),
      LessThanColumnPredicate.class, GreaterThanColumnPredicate.class));

    assertFalse(leavesAreAll(and(gt(NUMERIC_COLUMN, TEN), lt(NUMERIC_COLUMN, TEN)),
      EqualsColumnPredicate.class, NotEqualsColumnPredicate.class));
  }

  private <T> boolean leavesAreAll(FilterPredicate predicate, Class ... types) {
    Set<Class> validTypes = new HashSet<>();
    Collections.addAll(validTypes, types);

    IngestTree ingestTree = TestTools.generateIngestTreeFromSchema(SINGLE_COLUMN_SCHEMA, predicate);
    PredicateAnalysis analysis = new PredicateAnalysis(ingestTree);
    for (ColumnPredicate.LeafColumnPredicate leaf : analysis.leaves()) {
      if(!validTypes.contains(leaf.getClass())) {
        return false;
      }
    }

    return true;
  }

  @Test
  public void testColumnsRetrieval() {
    Operators.IntColumn first = intColumn("first");
    Operators.IntColumn second = intColumn("second");
    Operators.IntColumn third = intColumn("third");
    Operators.IntColumn fourth = intColumn("fourth");
    Operators.IntColumn fifth = intColumn("fifth");

    assertTrue(assertContainsAllColumns(and(eq(first, 7), notEq(second, 8)), first, second));
    assertTrue(assertContainsAllColumns(and(or(lt(third, 10), eq(fourth, 7)), notEq(fifth, 8)), third, fourth, fifth));
    assertTrue(assertContainsAllColumns(or(and(gt(first, 10), ltEq(third, 7)), eq(fifth, 8)), first, third, fifth));
  }

  private boolean assertContainsAllColumns (FilterPredicate predicate, Operators.IntColumn ... columns) {
    Set<ColumnDescriptor> set = new HashSet<ColumnDescriptor>();
    for (Operators.IntColumn column : columns) {
      set.add(TestTools.getColumnFromSchema(FIVE_COLUMN_SCHEMA, column.getColumnPath().toDotString()));
    }

    IngestTree ingestTree = TestTools.generateIngestTreeFromSchema(FIVE_COLUMN_SCHEMA, predicate);
    PredicateAnalysis analysis = new PredicateAnalysis(ingestTree);

    if(analysis.columns().size() !=  set.size()) {
      return false;
    }

    for (ColumnDescriptor column : analysis.columns()) {
      if (!set.contains(column)) {
        return false;
      }
    }

    return true;
  }

  @Test(expected=IllegalArgumentException.class)
  public void testColumnNotInSchema() {
    Operators.IntColumn notInSchema = intColumn("not_in_schema");
    Operators.Eq<Integer> equalsColumnNotInSchema = eq(notInSchema, 10);

    IngestTree ingestTree = TestTools.generateIngestTreeFromSchema(SINGLE_COLUMN_SCHEMA, equalsColumnNotInSchema);
    PredicateAnalysis analysis = new PredicateAnalysis(ingestTree);
  }
}
