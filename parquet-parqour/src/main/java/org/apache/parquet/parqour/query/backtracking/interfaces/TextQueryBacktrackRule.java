package org.apache.parquet.parqour.query.backtracking.interfaces;

/**
 * Created by sircodesalot on 15/4/2.
 */

import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;

public interface TextQueryBacktrackRule {
  Iterable<TextQueryExpressionType> launchForTokensOfType();

  boolean isMatch(TextQueryExpression parent, TextQueryLexer lexer);
  TextQueryExpression read(TextQueryExpression parent, TextQueryLexer lexer);
}
