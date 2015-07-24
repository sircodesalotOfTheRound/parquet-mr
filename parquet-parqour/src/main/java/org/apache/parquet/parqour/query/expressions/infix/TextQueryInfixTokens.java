package org.apache.parquet.parqour.query.expressions.infix;

import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryPunctuationToken;
import org.apache.parquet.parqour.query.tokens.TextQueryToken;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by sircodesalot on 7/24/15.
 */
public class TextQueryInfixTokens {
  private static final String AND = "and";
  private static final String OR = "or";

  private static Set<String> tokens = generateTokens();

  private static Set<String> generateTokens() {
    Set<String> tokens = new HashSet<String>();
    tokens.add(AND);
    tokens.add(OR);

    tokens.add(TextQueryPunctuationToken.PLUS);
    tokens.add(TextQueryPunctuationToken.MINUS);
    tokens.add(TextQueryPunctuationToken.MULTIPLY);
    tokens.add(TextQueryPunctuationToken.DIVIDE);
    tokens.add(TextQueryPunctuationToken.EQUALS);
    tokens.add(TextQueryPunctuationToken.NOT_EQUALS);
    tokens.add(TextQueryPunctuationToken.LESS_THAN);
    tokens.add(TextQueryPunctuationToken.LESS_THAN_OR_EQUALS);
    tokens.add(TextQueryPunctuationToken.GREATER_THAN);
    tokens.add(TextQueryPunctuationToken.GREATER_THAN_OR_EQUALS);

    return tokens;
  }

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
