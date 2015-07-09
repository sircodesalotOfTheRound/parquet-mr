package org.apache.parquet.parqour.query.expressions.pql;

import org.apache.parquet.parqour.ingest.read.iterator.lamba.Predicate;
import org.apache.parquet.parqour.query.backtracking.ParquelSelectStatementBacktrackRule;
import org.apache.parquet.parqour.query.backtracking.interfaces.ParquelBacktrackingRuleSet;
import org.apache.parquet.parqour.query.backtracking.rules.ParquelFromExpressionBacktrackRule;
import org.apache.parquet.parqour.query.backtracking.rules.ParquelFullyQualifiedNameExpressionBacktrackRule;
import org.apache.parquet.parqour.query.collections.ParquelAppendableCollection;
import org.apache.parquet.parqour.query.collections.ParquelCollection;
import org.apache.parquet.parqour.query.expressions.ParquelExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class ParquelTreeRootExpression extends ParquelExpression {
  private static final String ROOT = "(ROOT)";
  private static final ParquelBacktrackingRuleSet<ParquelExpression> rules = new ParquelBacktrackingRuleSet<ParquelExpression>()
    .add(new ParquelFullyQualifiedNameExpressionBacktrackRule())
    .add(new ParquelSelectStatementBacktrackRule())
    .add(new ParquelFromExpressionBacktrackRule());

  private final ParquelCollection<ParquelExpression> expressions;

  public ParquelTreeRootExpression(ParquelLexer lexer) {
    super(null, lexer, ParquelExpressionType.ROOT);

    this.expressions = readExpressions(lexer);
  }

  private ParquelCollection<ParquelExpression> readExpressions(ParquelLexer lexer) {
    ParquelAppendableCollection<ParquelExpression> expressions = new ParquelAppendableCollection<ParquelExpression>();
    while (!lexer.isEof()) {
      ParquelExpression expression = rules.read(this, lexer);

      if (expression != null) {
        expressions.add(expression);
      } else {
        expressions.add(new ParquelUnknownExpression(this, lexer));
      }
    }

    return expressions;
  }

  public ParquelCollection<ParquelExpression> expressions() {
    return this.expressions;
  }

  public ParquelExpression read(ParquelLexer lexer) {
    return new ParquelTreeRootExpression(lexer);
  }

  public boolean containsSelectExpression() {
    return expressions.any(new Predicate<ParquelExpression>() {
      @Override
      public boolean test(ParquelExpression expression) {
        return expression.is(ParquelExpressionType.SELECT);
      }
    });
  }

  public ParquelSelectStatement asSelectStatement() {
    return expressions.where(new Predicate<ParquelExpression>() {
      @Override
      public boolean test(ParquelExpression expression) {
        return expression.is(ParquelExpressionType.SELECT);
      }
    }).firstAs(ParquelSelectStatement.class);
  }

  public boolean isFqnExpression() {
    return expressions.all(new Predicate<ParquelExpression>() {
      @Override
      public boolean test(ParquelExpression expression) {
        return expression.is(ParquelExpressionType.FQN);
      }
    });
  }

  public ParquelFullyQualifiedNameExpression asFqnExpression() {
    return expressions.firstAs(ParquelFullyQualifiedNameExpression.class);
  }

  public boolean isSelectStatement() {
    return expressions.all(new Predicate<ParquelExpression>() {
      @Override
      public boolean test(ParquelExpression expression) {
        return expression.is(ParquelExpressionType.SELECT);
      }
    });
  }
/*
  @Override
  public void accept(ParquelNoReturnVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public ParquelCollection<ParquelExpression> children() {
    return this.expressions;
  }*/

  @Override
  public String toString() {
    return ROOT;
  }

  public static ParquelTreeRootExpression fromString(String pql) {
    ParquelLexer lexer = new ParquelLexer(pql, true);
    return new ParquelTreeRootExpression(lexer);
  }
}
