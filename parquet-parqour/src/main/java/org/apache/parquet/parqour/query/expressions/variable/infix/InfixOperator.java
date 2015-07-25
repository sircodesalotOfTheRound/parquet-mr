package org.apache.parquet.parqour.query.expressions.variable.infix;

import org.apache.parquet.parqour.exceptions.TextQueryException;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryPunctuationToken;
import org.apache.parquet.parqour.query.tokens.TextQueryToken;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by sircodesalot on 7/24/15.
 */
public enum InfixOperator {
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

  InfixOperator(String representation, int precedence) {
    this.representation = representation;
    this.precedence = precedence;
  }

  private static Map<String, InfixOperator> operators = generateTokens();

  private static Map<String, InfixOperator> generateTokens() {
    Map<String, InfixOperator> operators = new HashMap<String, InfixOperator>();
    operators.put(AND.toString(), AND);
    operators.put(OR.toString(), OR);

    operators.put(PLUS.toString(), PLUS);
    operators.put(MINUS.toString(), MINUS);
    operators.put(MULTIPLY.toString(), MULTIPLY);
    operators.put(DIVIDE.toString(), DIVIDE);
    operators.put(EQUALS.toString(), EQUALS);
    operators.put(NOT_EQUALS.toString(), NOT_EQUALS);
    operators.put(LESS_THAN.toString(), LESS_THAN);
    operators.put(LESS_THAN_OR_EQUALS.toString(), LESS_THAN_OR_EQUALS);
    operators.put(GREATER_THAN.toString(), GREATER_THAN);
    operators.put(GREATER_THAN_OR_EQUALS.toString(), GREATER_THAN_OR_EQUALS);

    return operators;
  }

  public int precedence() { return this.precedence; }

  @Override
  public String toString() { return this.representation; }

  public static boolean isInfixToken(String token) {
    return operators.containsKey(token.toLowerCase());
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

  public static InfixOperator readInfixOperator(TextQueryLexer lexer) {
    if (InfixOperator.isInfixToken(lexer)) {
      TextQueryToken token = lexer.readCurrentAndAdvance();
      return operators.get(token.toString());
    } else {
      throw new TextQueryException("Invalid token for infix expression");
    }
  }
}
