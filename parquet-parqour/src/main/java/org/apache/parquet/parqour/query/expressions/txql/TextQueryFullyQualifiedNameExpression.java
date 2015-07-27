package org.apache.parquet.parqour.query.expressions.txql;

import org.apache.parquet.parqour.query.backtracking.interfaces.TextQueryBacktrackingRuleSet;
import org.apache.parquet.parqour.query.backtracking.rules.TextQueryIdentifierExpressionBacktrackRule;
import org.apache.parquet.parqour.query.backtracking.rules.TextQueryWildcardExpressionBacktrackRule;
import org.apache.parquet.parqour.query.collections.TextQueryAppendableCollection;
import org.apache.parquet.parqour.query.collections.TextQueryCollection;
import org.apache.parquet.parqour.query.delimiters.TextQueryDotExpression;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryMemberExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Created by sircodesalot on 15/4/9.
 */
public class TextQueryFullyQualifiedNameExpression extends TextQueryVariableExpression {
  private static final TextQueryBacktrackingRuleSet<TextQueryMemberExpression> memberTypeRules = new TextQueryBacktrackingRuleSet<TextQueryMemberExpression>()
    .add(new TextQueryIdentifierExpressionBacktrackRule())
    .add(new TextQueryWildcardExpressionBacktrackRule());

  private final TextQueryCollection<TextQueryMemberExpression> members;
  private final String representation;

  public TextQueryFullyQualifiedNameExpression(TextQueryExpression parent, TextQueryLexer lexer) {
    super(parent, lexer, TextQueryExpressionType.FQN);

    this.members = this.readMembers(lexer);
    this.representation = generateRepresentation();
  }

  private TextQueryCollection<TextQueryMemberExpression> readMembers(TextQueryLexer lexer) {
    TextQueryAppendableCollection<TextQueryMemberExpression> identifiers = new TextQueryAppendableCollection<TextQueryMemberExpression>();
    while (!lexer.isEof()) {
      if (memberTypeRules.canParse(this, lexer)) {
        identifiers.add(memberTypeRules.read(this, lexer));
      } else {
        break;
      }

      if (TextQueryDotExpression.canRead(this, lexer)) {
        TextQueryDotExpression.read(this, lexer);
      } else {
        break;
      }

    }

    return identifiers;
  }

  private String generateRepresentation() {
    StringBuilder builder = new StringBuilder();
    for (TextQueryMemberExpression member : members) {
      if (builder.length() > 0) {
        builder.append(".");
      }
      builder.append(member);
    }

    return builder.toString();
  }

  public TextQueryCollection<TextQueryMemberExpression> members() {
    return this.members;
  }

  public static TextQueryFullyQualifiedNameExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return new TextQueryFullyQualifiedNameExpression(parent, lexer);
  }

  @Override
  public TextQueryVariableExpression simplify(TextQueryExpression parent) {
    return null;
  }

  @Override
  public TextQueryVariableExpression negate() {
    throw new NotImplementedException();
  }

  @Override
  public String toString() {
    return this.representation;
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }

  @Override
  public boolean equals(Object rhs) {
    if (rhs instanceof TextQueryFullyQualifiedNameExpression) {
      return this.representation.equals(((TextQueryFullyQualifiedNameExpression) rhs).representation);
    } if (rhs instanceof String) {
      return this.representation.equals(rhs);
    }

    return false;
  }
}
