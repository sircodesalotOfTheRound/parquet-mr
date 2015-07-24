package org.apache.parquet.parqour.query.backtracking.interfaces;

import org.apache.parquet.parqour.query.lexing.TextQueryLexer;

/**
 * Created by sircodesalot on 7/24/15.
 */
public interface TextQueryBacktrackingCallback {
  boolean apply(TextQueryLexer lexer);
}
