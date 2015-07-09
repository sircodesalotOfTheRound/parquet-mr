package org.apache.parquet.parqour.ingest.plan.predicates;

import org.apache.parquet.parqour.ingest.plan.predicates.traversal.TraversalInfo;
import org.apache.parquet.parqour.ingest.plan.predicates.types.ColumnPredicateNodeCategory;
import org.apache.parquet.parqour.ingest.plan.predicates.types.ColumnPredicateType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.PrimitiveType;

/**
 * Created by sircodesalot on 6/16/15.
 */
public class TautologicalColumnPredicate extends ColumnPredicate.LeafColumnPredicate {
  public static TautologicalColumnPredicate INSTANCE = new TautologicalColumnPredicate();

  private TautologicalColumnPredicate() {
    super(null, emptyColumnDescriptor(), ColumnPredicateNodeCategory.SYSTEM_DEFINED_LEAF, ColumnPredicateType.NONE);
  }

  private static ColumnDescriptor emptyColumnDescriptor() {
    return new ColumnDescriptor(new String[0], PrimitiveType.PrimitiveTypeName.INT32, 0, 0);
  }

  @Override
  public boolean test(Comparable entry) {
    return true;
  }

  @Override
  public TraversalInfo traversalInfo() {
    return new TraversalInfo(this);
  }
}

