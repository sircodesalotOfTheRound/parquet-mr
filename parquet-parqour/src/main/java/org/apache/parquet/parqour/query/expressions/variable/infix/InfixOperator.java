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
  AS("as", 0),
  AND("and", 1),
  OR("or", 1),
  BETWEEN("between", 2),
  MATCHES("matches", 2),
  IN("in", 2),
  IS("is", 2),
  EQUALS(TextQueryPunctuationToken.EQUALS, 3),
  NOT_EQUALS(TextQueryPunctuationToken.NOT_EQUALS, 3),
  LESS_THAN(TextQueryPunctuationToken.LESS_THAN, 3),
  LESS_THAN_OR_EQUALS(TextQueryPunctuationToken.LESS_THAN_OR_EQUALS, 3),
  GREATER_THAN(TextQueryPunctuationToken.GREATER_THAN, 3),
  GREATER_THAN_OR_EQUALS(TextQueryPunctuationToken.GREATER_THAN_OR_EQUALS, 3),
  PLUS(TextQueryPunctuationToken.PLUS, 4),
  MINUS(TextQueryPunctuationToken.MINUS, 4),
  MULTIPLY(TextQueryPunctuationToken.MULTIPLY, 5),
  DIVIDE(TextQueryPunctuationToken.DIVIDE, 5),
  MODULO(TextQueryPunctuationToken.MODULO, 5);

  private final String representation;
  private final Integer precedence;

  InfixOperator(String representation, int precedence) {
    this.representation = representation;
    this.precedence = precedence;
  }

  private static Map<String, InfixOperator> operators = generateTokens();

  private static Map<String, InfixOperator> generateTokens() {
    Map<String, InfixOperator> operators = new HashMap<String, InfixOperator>();
    for (InfixOperator value : InfixOperator.values()) {
      operators.put(value.toString(), value);
    }

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
