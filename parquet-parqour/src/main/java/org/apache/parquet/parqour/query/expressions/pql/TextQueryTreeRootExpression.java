package org.apache.parquet.parqour.query.expressions.pql;

import org.apache.parquet.parqour.ingest.read.iterator.lamba.Predicate;
import org.apache.parquet.parqour.query.backtracking.ParquelSelectStatementBacktrackRule;
import org.apache.parquet.parqour.query.backtracking.interfaces.ParquelBacktrackingRuleSet;
import org.apache.parquet.parqour.query.backtracking.rules.ParquelFromExpressionBacktrackRule;
import org.apache.parquet.parqour.query.backtracking.rules.ParquelFullyQualifiedNameExpressionBacktrackRule;
import org.apache.parquet.parqour.query.collections.TextQueryAppendableCollection;
import org.apache.parquet.parqour.query.collections.TextQueryCollection;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class TextQueryTreeRootExpression extends TextQueryExpression {
  private static final String ROOT = "(ROOT)";
  private static final ParquelBacktrackingRuleSet<TextQueryExpression> rules = new ParquelBacktrackingRuleSet<TextQueryExpression>()
    .add(new ParquelFullyQualifiedNameExpressionBacktrackRule())
    .add(new ParquelSelectStatementBacktrackRule())
    .add(new ParquelFromExpressionBacktrackRule());

  private final TextQueryCollection<TextQueryExpression> expressions;

  public TextQueryTreeRootExpression(ParquelLexer lexer) {
    super(null, lexer, ParquelExpressionType.ROOT);

    this.expressions = readExpressions(lexer);
  }

  private TextQueryCollection<TextQueryExpression> readExpressions(ParquelLexer lexer) {
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

  public TextQueryExpression read(ParquelLexer lexer) {
    return new TextQueryTreeRootExpression(lexer);
  }

  public boolean containsSelectExpression() {
    return expressions.any(new Predicate<TextQueryExpression>() {
      @Override
      public boolean test(TextQueryExpression expression) {
        return expression.is(ParquelExpressionType.SELECT);
      }
    });
  }

  public TextQuerySelectStatement asSelectStatement() {
    return expressions.where(new Predicate<TextQueryExpression>() {
      @Override
      public boolean test(TextQueryExpression expression) {
        return expression.is(ParquelExpressionType.SELECT);
      }
    }).firstAs(TextQuerySelectStatement.class);
  }

  public boolean isFqnExpression() {
    return expressions.all(new Predicate<TextQueryExpression>() {
      @Override
      public boolean test(TextQueryExpression expression) {
        return expression.is(ParquelExpressionType.FQN);
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
        return expression.is(ParquelExpressionType.SELECT);
      }
    });
  }

  @Override
  public String toString() {
    return ROOT;
  }

  public static TextQueryTreeRootExpression fromString(String pql) {
    ParquelLexer lexer = new ParquelLexer(pql, true);
    return new TextQueryTreeRootExpression(lexer);
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return visitor.visit(this);
  }

}
