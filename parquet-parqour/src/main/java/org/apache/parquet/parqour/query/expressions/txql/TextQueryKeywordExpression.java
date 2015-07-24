package org.apache.parquet.parqour.query.expressions.txql;


import org.apache.parquet.parqour.exceptions.TextQueryException;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryIdentifierToken;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by sircodesalot on 15/4/2.
 */
public abstract class TextQueryKeywordExpression extends TextQueryExpression {
  private static final Set<String> keywords = generateKeywordSet();

  public static final String SELECT = "SELECT";
  public static final String FROM = "FROM";
  public static final String WHERE = "WHERE";
  public static final String USE = "USE";
  public static final String CREATE = "CREATE";
  public static final String DROP = "DROP";
  public static final String TABLE = "TABLE";
  public static final String COMMENT = "COMMENT";

  private final TextQueryIdentifierToken token;

  public TextQueryKeywordExpression(TextQueryExpression parent, TextQueryLexer lexer, TextQueryExpressionType type) {
    super(parent, lexer, type);

    this.token = readToken(lexer);
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }

  private TextQueryIdentifierToken readToken(TextQueryLexer lexer) {
    if (!isKeyword(lexer)) {
      throw new TextQueryException("Keyword expressions must be keywords");
    }

    return (TextQueryIdentifierToken)lexer.current();
  }

  public static boolean isKeyword(TextQueryLexer lexer) {
    return isKeyword(lexer.current().toString());
  }

  public static boolean isKeyword(String identifier) {
    return keywords.contains(identifier.toUpperCase());
  }

  private static Set<String> generateKeywordSet() {
    Set<String> keywords = new HashSet<String>();
    keywords.add(SELECT);
    keywords.add(FROM);
    keywords.add(WHERE);
    keywords.add(USE);
    keywords.add(CREATE);
    keywords.add(DROP);

    return keywords;
  }
}
