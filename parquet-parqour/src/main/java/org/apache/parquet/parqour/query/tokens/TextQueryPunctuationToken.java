package org.apache.parquet.parqour.query.tokens;


import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelCharacterStream;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class TextQueryPunctuationToken extends TextQueryToken {
  public static final String COMMA = ",";
  public static final String OPEN_PARENS = "(";
  public static final String CLOSE_PARENS = ")";
  public static final String OPEN_BRACE = "{";
  public static final String CLOSE_BRACE = "}";
  public static final String OPEN_DIAMOND = "<";
  public static final String CLOSE_DIAMOND = ">";
  public static final String COLON = ":";
  public static final String EQUALS = "=";
  public static final String NOT_EQUALS = "!=";
  public static final String DOT = ".";
  public static final String WILDCARD = "*";
  public static final String DOLLAR = "$";
  public static final String SINGLE_QUOTE = "'";

  private final String token;

  private TextQueryPunctuationToken(ParquelCharacterStream stream) {
    super(stream.position(), TextQueryExpressionType.PUNCTUATION);
    this.token = parseFromString(stream);
  }

  private String parseFromString(ParquelCharacterStream stream) {
    StringBuilder builder = new StringBuilder();
    char first = stream.readCurrentAndAdvance();
    builder.append(first);

    if (first == '!' && !stream.isEof() && stream.currentIs('=')) {
      builder.append(stream.readCurrentAndAdvance());
    }

    return builder.toString();
  }


  public static TextQueryPunctuationToken read(ParquelCharacterStream stream) {
    return new TextQueryPunctuationToken(stream);
  }

  @Override
  public String toString() {
    return token;
  }

  public String token() { return this.token; }
}
