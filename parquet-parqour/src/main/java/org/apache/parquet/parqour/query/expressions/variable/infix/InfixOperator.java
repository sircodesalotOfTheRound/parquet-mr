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
  PLUS(TextQueryPunctuationToken.PLUS, 3),
  MINUS(TextQueryPunctuationToken.MINUS, 3),
  MULTIPLY(TextQueryPunctuationToken.MULTIPLY, 4),
  DIVIDE(TextQueryPunctuationToken.DIVIDE, 4),
  MODULO(TextQueryPunctuationToken.MODULO, 4),
  EQUALS(TextQueryPunctuationToken.EQUALS, 2),
  NOT_EQUALS(TextQueryPunctuationToken.NOT_EQUALS, 2),
  LESS_THAN(TextQueryPunctuationToken.LESS_THAN, 2),
  LESS_THAN_OR_EQUALS(TextQueryPunctuationToken.LESS_THAN_OR_EQUALS, 2),
  GREATER_THAN(TextQueryPunctuationToken.GREATER_THAN, 2),
  GREATER_THAN_OR_EQUALS(TextQueryPunctuationToken.GREATER_THAN_OR_EQUALS, 2);

  private final String representation;
  private final Integer precedence;

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
    operators.put(MODULO.toString(), MODULO);
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

  public boolean hasHigherPrecedenceThan(InfixOperator rhs) {
    return this.precedence.compareTo(rhs.precedence) > 0;
  }
}
