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

  BOOLEAN,
  NULL,

  INFIX,
  PARENTHETICAL,
  MATCHES,

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
  UDF,

  EQUALS,
  NOT_EQUALS,
  LESS_THAN,
  LESS_THAN_OR_EQUALS,
  GREATER_THAN,
  GREATER_THAN_OR_EQUALS,

  AND,
  OR
}
