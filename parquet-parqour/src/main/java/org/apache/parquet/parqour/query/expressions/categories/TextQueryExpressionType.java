package org.apache.parquet.parqour.query.expressions.categories;

/**
 * Created by sircodesalot on 6/28/15.
 */
public enum TextQueryExpressionType {
  DOT,
  COMMA,

  IDENTIFIER,
  FQN,

  TABLE,
  TABLE_SET,
  STRING,

  NAMED_COLUMN,
  WILDCARD,
  COLUMN_SET,

  INFIX,
  PARENTHETICAL,

  KEYWORD,
  SELECT,
  FROM,
  WHERE,

  UNKNOWN,
  ROOT,
  NOT,

  WHITESPACE,
  PUNCTUATION,

  NUMERIC,
  UDF
}
