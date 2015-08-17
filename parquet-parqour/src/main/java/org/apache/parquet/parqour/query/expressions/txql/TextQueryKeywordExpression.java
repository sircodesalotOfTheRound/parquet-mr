package org.apache.parquet.parqour.query.expressions.txql;


import org.apache.parquet.parqour.exceptions.TextQueryException;
import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.lexing.TextQueryLexer;
import org.apache.parquet.parqour.query.tokens.TextQueryIdentifierToken;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;
import org.apache.parquet.parqour.tools.TransformCollection;
import org.apache.parquet.parqour.tools.TransformList;

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
  public static final String NOT = "NOT";
  public static final String AND = "AND";
  public static final String OR = "OR";
  public static final String BETWEEN = "OR";
  public static final String MATCHES = "OR";
  public static final String IN = "OR";
  public static final String TRUE = "TRUE";
  public static final String FALSE = "FALSE";
  public static final String NULL = "NULL";

  private final TextQueryIdentifierToken token;

  public TextQueryKeywordExpression(TextQueryExpression parent, TextQueryLexer lexer, TextQueryExpressionType type) {
    super(parent, lexer, type);

    this.token = readToken(lexer);
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }

  @Override
  public TransformCollection<String> collectColumnDependencies(TransformList<String> collectTo) {
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
    keywords.add(AND);
    keywords.add(OR);
    keywords.add(NOT);

    return keywords;
  }
}
