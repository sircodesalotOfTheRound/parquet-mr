package org.apache.parquet.parqour.query.expressions.pql;


import org.apache.parquet.parqour.exceptions.ParquelException;
import org.apache.parquet.parqour.query.expressions.ParquelExpression;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.lexing.ParquelLexer;
import org.apache.parquet.parqour.query.tokens.ParquelIdentifierToken;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by sircodesalot on 15/4/2.
 */
public abstract class ParquelKeywordExpression extends ParquelExpression {
  private static final Set<String> keywords = generateKeywordSet();

  public static final String SELECT = "SELECT";
  public static final String FROM = "FROM";
  public static final String WHERE = "WHERE";
  public static final String USE = "USE";
  public static final String CREATE = "CREATE";
  public static final String DROP = "DROP";
  public static final String TABLE = "TABLE";
  public static final String COMMENT = "COMMENT";

  private final ParquelIdentifierToken token;

  public ParquelKeywordExpression(ParquelExpression parent, ParquelLexer lexer, ParquelExpressionType type) {
    super(parent, lexer, type);

    this.token = readToken(lexer);
  }

  private ParquelIdentifierToken readToken(ParquelLexer lexer) {
    if (!isKeyword(lexer)) {
      throw new ParquelException("Keyword expressions must be keywords");
    }

    return (ParquelIdentifierToken)lexer.current();
  }

  public static boolean isKeyword(ParquelLexer lexer) {
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
