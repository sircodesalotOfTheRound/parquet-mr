package org.apache.parquet.parqour.query.expressions.pql;

import org.apache.parquet.parqour.exceptions.ParquelException;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelMemberExpression;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryIdentifierToken;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class TextQueryIdentifierExpression extends TextQueryExpression implements ParquelMemberExpression {
  private final TextQueryIdentifierToken identifier;
  //private final ParquelGenericParameterListExpression genericParameters;
  private final String representation;
  //private final ParquelCollection<ParquelExpression> children;

  public TextQueryIdentifierExpression(TextQueryExpression parent, TextQueryLexer lexer) {
    super(parent, lexer, TextQueryExpressionType.IDENTIFIER);

    this.identifier = readIdentifier(lexer);
    //this.genericParameters = readGenericParameters(lexer);
    this.representation = generateRepresentation();
    //this.children = new ParquelAppendableCollection<ParquelExpression>(genericParameters);
  }

  private TextQueryIdentifierToken readIdentifier(TextQueryLexer lexer) {
    if (!lexer.currentIs(TextQueryExpressionType.IDENTIFIER)) {
      throw new ParquelException("Identifiers must start with Identifier tokens. Found %s", lexer.current());
    }

    return lexer.readCurrentAndAdvance(TextQueryExpressionType.IDENTIFIER);
  }
/*
  private ParquelGenericParameterListExpression readGenericParameters(ParquelLexer lexer) {
    if (ParquelGenericParameterListExpression.canRead(this, lexer)) {
      return ParquelGenericParameterListExpression.read(this, lexer);
    } else {
      return null;
    }
  }*/

  private String generateRepresentation() {
    StringBuilder builder = new StringBuilder();
    builder.append(identifier);
    /*if (hasGenericParamters()) {
      builder.append(genericParameters);
    }*/
    return builder.toString();
  }

  public String identifier() {
    return identifier.toString();
  }

  /*
  public boolean hasGenericParamters() {
    return this.genericParameters != null;
  }*/
/*
  public ParquelGenericParameterListExpression genericParameters() {
    return this.genericParameters;
  }
  @Override
  public void accept(ParquelNoReturnVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public ParquelCollection<ParquelExpression> children() {
    return this.children;
  }*/

  public static TextQueryIdentifierExpression read(TextQueryExpression parent, TextQueryLexer lexer) {
    return new TextQueryIdentifierExpression(parent, lexer);
  }

  @Override
  public String toString() {
    return this.representation;
  }

  public static boolean canRead(TextQueryExpression parent, TextQueryLexer lexer) {
    return lexer.currentIs(TextQueryExpressionType.IDENTIFIER);
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }

}
