package org.apache.parquet.parqour.query.expressions.pql;

import org.apache.parquet.parqour.query.backtracking.interfaces.ParquelBacktrackingRuleSet;
import org.apache.parquet.parqour.query.backtracking.rules.ParquelIdentifierExpressionBacktrackRule;
import org.apache.parquet.parqour.query.backtracking.rules.ParquelWildcardExpressionBacktrackRule;
import org.apache.parquet.parqour.query.collections.TextQueryAppendableCollection;
import org.apache.parquet.parqour.query.collections.TextQueryCollection;
import org.apache.parquet.parqour.query.delimiters.TextQueryDotExpression;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.ParquelMemberExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 15/4/9.
 */
public class TextQueryFullyQualifiedNameExpression extends TextQueryVariableExpression {
  private static final ParquelBacktrackingRuleSet<ParquelMemberExpression> memberTypeRules = new ParquelBacktrackingRuleSet<ParquelMemberExpression>()
    .add(new ParquelIdentifierExpressionBacktrackRule())
    .add(new ParquelWildcardExpressionBacktrackRule());

  private final TextQueryCollection<ParquelMemberExpression> members;
  private final String representation;

  public TextQueryFullyQualifiedNameExpression(TextQueryExpression parent, ParquelLexer lexer) {
    super(parent, lexer, ParquelExpressionType.FQN);

    this.members = this.readMembers(lexer);
    this.representation = generateRepresentation();
  }

  private TextQueryCollection<ParquelMemberExpression> readMembers(ParquelLexer lexer) {
    TextQueryAppendableCollection<ParquelMemberExpression> identifiers = new TextQueryAppendableCollection<ParquelMemberExpression>();
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
    for (ParquelMemberExpression member : members) {
      if (builder.length() > 0) {
        builder.append(".");
      }
      builder.append(member);
    }

    return builder.toString();
  }

  public TextQueryCollection<ParquelMemberExpression> members() {
    return this.members;
  }

/*
  @Override
  public void accept(ParquelNoReturnVisitor visitor) {

  }

  @Override
  public ParquelCollection<ParquelExpression> children() {
    return members.castTo(ParquelExpression.class);
  }
  */

  public static TextQueryFullyQualifiedNameExpression read(TextQueryExpression parent, ParquelLexer lexer) {
    return new TextQueryFullyQualifiedNameExpression(parent, lexer);
  }

  @Override
  public String toString() {
    return this.representation;
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }

}
