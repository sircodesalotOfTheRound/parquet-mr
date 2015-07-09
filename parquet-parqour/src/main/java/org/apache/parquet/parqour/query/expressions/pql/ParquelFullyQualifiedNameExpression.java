package org.apache.parquet.parqour.query.expressions.pql;

import org.apache.parquet.parqour.query.backtracking.interfaces.ParquelBacktrackingRuleSet;
import org.apache.parquet.parqour.query.backtracking.rules.ParquelIdentifierExpressionBacktrackRule;
import org.apache.parquet.parqour.query.backtracking.rules.ParquelWildcardExpressionBacktrackRule;
import org.apache.parquet.parqour.query.collections.ParquelAppendableCollection;
import org.apache.parquet.parqour.query.collections.ParquelCollection;
import org.apache.parquet.parqour.query.delimiters.ParquelDotExpression;
import org.apache.parquet.parqour.query.expressions.ParquelExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.ParquelMemberExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelVariableExpression;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;

/**
 * Created by sircodesalot on 15/4/9.
 */
public class ParquelFullyQualifiedNameExpression extends ParquelVariableExpression {
  private static final ParquelBacktrackingRuleSet<ParquelMemberExpression> memberTypeRules = new ParquelBacktrackingRuleSet<ParquelMemberExpression>()
    .add(new ParquelIdentifierExpressionBacktrackRule())
    .add(new ParquelWildcardExpressionBacktrackRule());

  private final ParquelCollection<ParquelMemberExpression> members;
  private final String representation;

  public ParquelFullyQualifiedNameExpression(ParquelExpression parent, ParquelLexer lexer) {
    super(parent, lexer, ParquelExpressionType.FQN);

    this.members = this.readMembers(lexer);
    this.representation = generateRepresentation();
  }

  private ParquelCollection<ParquelMemberExpression> readMembers(ParquelLexer lexer) {
    ParquelAppendableCollection<ParquelMemberExpression> identifiers = new ParquelAppendableCollection<ParquelMemberExpression>();
    while (!lexer.isEof()) {
      if (memberTypeRules.canParse(this, lexer)) {
        identifiers.add(memberTypeRules.read(this, lexer));
      } else {
        break;
      }

      if (ParquelDotExpression.canRead(this, lexer)) {
        ParquelDotExpression.read(this, lexer);
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

  public ParquelCollection<ParquelMemberExpression> members() {
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

  public static ParquelFullyQualifiedNameExpression read(ParquelExpression parent, ParquelLexer lexer) {
    return new ParquelFullyQualifiedNameExpression(parent, lexer);
  }

  @Override
  public String toString() {
    return this.representation;
  }
}
