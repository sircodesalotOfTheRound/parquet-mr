package org.apache.parquet.parqour.query.expressions.txql;

import org.apache.parquet.parqour.ingest.read.iterator.lamba.Predicate;
import org.apache.parquet.parqour.query.backtracking.rules.TextQuerySelectStatementBacktrackRule;
import org.apache.parquet.parqour.query.backtracking.interfaces.TextQueryBacktrackingRuleSet;
import org.apache.parquet.parqour.query.backtracking.rules.TextQueryFromExpressionBacktrackRule;
import org.apache.parquet.parqour.query.backtracking.rules.TextQueryFullyQualifiedNameExpressionBacktrackRule;
import org.apache.parquet.parqour.query.collections.TextQueryAppendableCollection;
import org.apache.parquet.parqour.query.collections.TextQueryCollection;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class TextQueryTreeRootExpression extends TextQueryExpression {
  private static final String ROOT = "(ROOT)";
  private static final TextQueryBacktrackingRuleSet<TextQueryExpression> rules = new TextQueryBacktrackingRuleSet<TextQueryExpression>()
    .add(new TextQueryFullyQualifiedNameExpressionBacktrackRule())
    .add(new TextQuerySelectStatementBacktrackRule())
    .add(new TextQueryFromExpressionBacktrackRule());

  private final TextQueryCollection<TextQueryExpression> expressions;
  private final String text;

  public TextQueryTreeRootExpression(TextQueryLexer lexer) {
    super(null, lexer, TextQueryExpressionType.ROOT);

    this.expressions = readExpressions(lexer);
    this.text = lexer.text();
  }

  private TextQueryCollection<TextQueryExpression> readExpressions(TextQueryLexer lexer) {
    TextQueryAppendableCollection<TextQueryExpression> expressions = new TextQueryAppendableCollection<TextQueryExpression>();
    while (!lexer.isEof()) {
      TextQueryExpression expression = rules.read(this, lexer);

      if (expression != null) {
        expressions.add(expression);
      } else {
        expressions.add(new TextQueryUnknownExpression(this, lexer));
      }
    }

    return expressions;
  }

  public TextQueryCollection<TextQueryExpression> expressions() {
    return this.expressions;
  }

  public TextQueryExpression read(TextQueryLexer lexer) {
    return new TextQueryTreeRootExpression(lexer);
  }

  public boolean containsSelectExpression() {
    return expressions.any(new Predicate<TextQueryExpression>() {
      @Override
      public boolean test(TextQueryExpression expression) {
        return expression.is(TextQueryExpressionType.SELECT);
      }
    });
  }

  public TextQuerySelectStatementExpression asSelectStatement() {
    return expressions.where(new Predicate<TextQueryExpression>() {
      @Override
      public boolean test(TextQueryExpression expression) {
        return expression.is(TextQueryExpressionType.SELECT);
      }
    }).firstAs(TextQuerySelectStatementExpression.class);
  }

  public boolean isFqnExpression() {
    return expressions.all(new Predicate<TextQueryExpression>() {
      @Override
      public boolean test(TextQueryExpression expression) {
        return expression.is(TextQueryExpressionType.FQN);
      }
    });
  }

  public TextQueryFullyQualifiedNameExpression asFqnExpression() {
    return expressions.firstAs(TextQueryFullyQualifiedNameExpression.class);
  }

  public boolean isSelectStatement() {
    return expressions.all(new Predicate<TextQueryExpression>() {
      @Override
      public boolean test(TextQueryExpression expression) {
        return expression.is(TextQueryExpressionType.SELECT);
      }
    });
  }

  @Override
  public String toString() {
    return ROOT;
  }
  public String text() { return this.text; }

  public static TextQueryTreeRootExpression fromString(String pql) {
    TextQueryLexer lexer = new TextQueryLexer(pql, true);
    return new TextQueryTreeRootExpression(lexer);
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return visitor.visit(this);
  }

}
