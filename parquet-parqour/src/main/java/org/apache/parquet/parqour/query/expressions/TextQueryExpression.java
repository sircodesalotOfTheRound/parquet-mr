package org.apache.parquet.parqour.query.expressions;

import org.apache.parquet.parqour.ingest.read.iterator.lamba.Predicate;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryToken;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;
import org.apache.parquet.parqour.tools.TransformCollection;
import org.apache.parquet.parqour.tools.TransformList;

import java.util.List;

/**
 * Created by sircodesalot on 15/4/2.
 */
public abstract class TextQueryExpression extends TextQueryToken {
  private final TextQueryLexer lexer;
  private final TextQueryExpressionType type;
  private TextQueryExpression parent;


  public TextQueryExpression(TextQueryExpression parent, TextQueryExpressionType type) {
    super(type);
    this.lexer = null;
    this.type = type;
    this.parent = parent;
  }

  public TextQueryExpression(TextQueryExpression parent, TextQueryLexer lexer, TextQueryExpressionType type) {
    super(lexer.position(), type);
    this.parent = parent;
    this.lexer = lexer;
    this.type = type;
  }

  public TextQueryExpression parent() {
    return this.parent;
  }

  public boolean hasParentWhere(Predicate<TextQueryExpression> predicate) {
    TextQueryExpression currentParent = this.parent;
    while (currentParent != null) {
      if (predicate.test(currentParent)) {
        return true;
      }
    }

    return false;
  }

  public abstract <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor);

  public void setParent(TextQueryExpression parent) {
    this.parent = parent;
  }

  public <T> T as(Class<T> type) { return (T)this; }

  public abstract TransformCollection<String> collectColumnDependencies(TransformList<String> collectTo);
}
