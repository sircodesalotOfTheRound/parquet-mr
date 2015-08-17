package org.apache.parquet.parqour.query.expressions.variable.negation;

import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryKeywordExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;
import org.apache.parquet.parqour.tools.TransformCollection;
import org.apache.parquet.parqour.tools.TransformList;

/**
 * Created by sircodesalot on 7/27/15.
 */
public class TextQueryNotExpression extends TextQueryVariableExpression {
  private final TextQueryVariableExpression negatedExpression;

  public TextQueryNotExpression(TextQueryExpression parent, TextQueryLexer lexer) {
    super(parent, lexer, TextQueryExpressionType.NOT);

    this.negatedExpression = readNegatedExpression(lexer);
  }

  private TextQueryVariableExpression readNegatedExpression(TextQueryLexer lexer) {
    lexer.readCurrentAndAdvance(TextQueryExpressionType.IDENTIFIER, TextQueryKeywordExpression.NOT);
    return TextQueryVariableExpression.read(this, lexer);
  }

  @Override
  public TextQueryVariableExpression simplify(TextQueryExpression parent) {
    this.negatedExpression.setParent(parent);
    return this.negatedExpression.negate();
  }

  @Override
  public TextQueryVariableExpression negate() {
    // Double negation.
    return this.negatedExpression;
  }

  public TextQueryVariableExpression negatedExpression() { return this.negatedExpression; }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }

  @Override
  public TransformCollection<String> collectColumnDependencies(TransformList<String> collectTo) {
    return negatedExpression.collectColumnDependencies(collectTo);
  }

  public static TextQueryNotExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return new TextQueryNotExpression(parent, lexer);
  }
}
