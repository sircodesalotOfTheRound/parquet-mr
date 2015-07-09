package org.apache.parquet.parqour.query.backtracking.interfaces;

/**
 * Created by sircodesalot on 15/4/2.
 */

import org.apache.parquet.parqour.query.expressions.ParquelExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;

public interface ParquelBacktrackRule {
  ParquelExpressionType launchForTokensOfType();

  boolean isMatch(ParquelExpression parent, ParquelLexer lexer);
  ParquelExpression read(ParquelExpression parent, ParquelLexer lexer);
}
