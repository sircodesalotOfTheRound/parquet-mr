package org.apache.parquet.parqour.query.expressions.pql;

import org.apache.parquet.parqour.exceptions.ParquelException;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelMemberExpression;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;
import org.apache.parquet.parqour.query.tokens.ParquelIdentifierToken;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class TextQueryIdentifierExpression extends TextQueryExpression implements ParquelMemberExpression {
  private final ParquelIdentifierToken identifier;
  //private final ParquelGenericParameterListExpression genericParameters;
  private final String representation;
  //private final ParquelCollection<ParquelExpression> children;

  public TextQueryIdentifierExpression(TextQueryExpression parent, ParquelLexer lexer) {
    super(parent, lexer, ParquelExpressionType.IDENTIFIER);

    this.identifier = readIdentifier(lexer);
    //this.genericParameters = readGenericParameters(lexer);
    this.representation = generateRepresentation();
    //this.children = new ParquelAppendableCollection<ParquelExpression>(genericParameters);
  }

  private ParquelIdentifierToken readIdentifier(ParquelLexer lexer) {
    if (!lexer.currentIs(ParquelExpressionType.IDENTIFIER)) {
      throw new ParquelException("Identifiers must start with Identifier tokens. Found %s", lexer.current());
    }

    return lexer.readCurrentAndAdvance(ParquelExpressionType.IDENTIFIER);
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

  public static TextQueryIdentifierExpression read(TextQueryExpression parent, ParquelLexer lexer) {
    return new TextQueryIdentifierExpression(parent, lexer);
  }

  @Override
  public String toString() {
    return this.representation;
  }

  public static boolean canRead(TextQueryExpression parent, ParquelLexer lexer) {
    return lexer.currentIs(ParquelExpressionType.IDENTIFIER);
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }

}
