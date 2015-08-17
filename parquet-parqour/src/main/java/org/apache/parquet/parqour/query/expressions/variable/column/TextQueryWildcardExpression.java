package org.apache.parquet.parqour.query.expressions.variable.column;

import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryMemberExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryPunctuationToken;
import org.apache.parquet.parqour.query.tokens.TextQueryToken;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;
import org.apache.parquet.parqour.tools.TransformCollection;
import org.apache.parquet.parqour.tools.TransformList;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class TextQueryWildcardExpression extends TextQueryColumnExpression implements TextQueryMemberExpression {
  public TextQueryToken token;

  public TextQueryWildcardExpression(TextQueryExpression parent, TextQueryLexer lexer) {
    super(parent, lexer, TextQueryExpressionType.WILDCARD);

    this.token = readToken(lexer);
  }

  private TextQueryToken readToken(TextQueryLexer lexer) {
    return lexer.readCurrentAndAdvance(TextQueryExpressionType.PUNCTUATION, TextQueryPunctuationToken.WILDCARD);
  }

  public static TextQueryWildcardExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return new TextQueryWildcardExpression(parent, lexer);
  }

  @Override
  public TextQueryVariableExpression simplify(TextQueryExpression parent) {
    return null;
  }

  @Override
  public TextQueryVariableExpression negate() {
    return null;
  }

  @Override
  public String toString() {
    return TextQueryPunctuationToken.WILDCARD;
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return visitor.visit(this);
  }

  @Override
  public TransformCollection<String> collectColumnDependencies(TransformList<String> collectTo) {
    throw new NotImplementedException();
  }

}
