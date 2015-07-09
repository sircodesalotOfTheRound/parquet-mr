package org.apache.parquet.parqour.ingest.plan.predicates.types;

/**
 * Created by sircodesalot on 6/8/15.
 */
public enum ColumnPredicateType {
  EQ,
  NEQ,
  LT,
  LTEQ,
  GT,
  GTEQ,
  AND,
  OR,
  NONE,
  USER_DEFINED,
}
