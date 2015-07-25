package org.apache.parquet.parqour.query.expressions.variable.infix;

import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryPunctuationToken;
import org.apache.parquet.parqour.query.tokens.TextQueryToken;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by sircodesalot on 7/24/15.
 */
public enum TextQueryInfixTokens {
  AND("and", 1),
  OR("or", 1),
  PLUS(TextQueryPunctuationToken.PLUS, 1),
  MINUS(TextQueryPunctuationToken.MINUS, 1),
  MULTIPLY(TextQueryPunctuationToken.MULTIPLY, 1),
  DIVIDE(TextQueryPunctuationToken.DIVIDE, 1),
  EQUALS(TextQueryPunctuationToken.EQUALS, 1),
  NOT_EQUALS(TextQueryPunctuationToken.NOT_EQUALS, 1),
  LESS_THAN(TextQueryPunctuationToken.LESS_THAN, 1),
  LESS_THAN_OR_EQUALS(TextQueryPunctuationToken.LESS_THAN_OR_EQUALS, 1),
  GREATER_THAN(TextQueryPunctuationToken.GREATER_THAN, 1),
  GREATER_THAN_OR_EQUALS(TextQueryPunctuationToken.GREATER_THAN_OR_EQUALS, 1);

  private final String representation;
  private final int precedence;

  TextQueryInfixTokens(String representation, int precedence) {
    this.representation = representation;
    this.precedence = precedence;
  }

  private static Set<String> tokens = generateTokens();

  private static Set<String> generateTokens() {
    Set<String> tokens = new HashSet<String>();
    tokens.add(AND.toString());
    tokens.add(OR.toString());

    tokens.add(PLUS.toString());
    tokens.add(MINUS.toString());
    tokens.add(MULTIPLY.toString());
    tokens.add(DIVIDE.toString());
    tokens.add(EQUALS.toString());
    tokens.add(NOT_EQUALS.toString());
    tokens.add(LESS_THAN.toString());
    tokens.add(LESS_THAN_OR_EQUALS.toString());
    tokens.add(GREATER_THAN.toString());
    tokens.add(GREATER_THAN_OR_EQUALS.toString());

    return tokens;
  }

  public int precedence() { return this.precedence; }

  @Override
  public String toString() { return this.representation; }

  public static boolean isInfixToken(String token) {
    return tokens.contains(token.toLowerCase());
  }

  public static boolean isInfixToken(TextQueryToken token) {
    return isInfixToken(token.toString());
  }

  public static boolean isInfixToken(TextQueryLexer lexer) {
    if (!lexer.isEof()) {
      return isInfixToken(lexer.current());
    }

    return false;
  }
}
