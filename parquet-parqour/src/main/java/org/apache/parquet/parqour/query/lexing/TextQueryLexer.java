package org.apache.parquet.parqour.query.lexing;


import org.apache.parquet.parqour.exceptions.TextQueryException;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.tokens.TextQueryToken;

import java.util.Stack;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class TextQueryLexer {
  private final String text;
  private final TextQueryTokenList tokens;
  private boolean skipWhitespaces;
  private int currentIndex;
  private Stack<Integer> undoStack;
  private Stack<Boolean> whitespaceStateStack;

  public TextQueryLexer(String text, boolean skipWhitespacesByDefault) {
    this.text = text;
    this.skipWhitespaces = skipWhitespacesByDefault;
    this.tokens = new TextQueryTokenList(text);
    this.undoStack = new Stack<Integer>();
    this.whitespaceStateStack = new Stack<Boolean>();
  }

  public String text() {
    return this.text;
  }

  public void advance() {
    if (isEof()) {
      throw new TextQueryException("Attempt to advance past end of file");
    }

    currentIndex += 1;

    if (skipWhitespaces) {
      while (!isEof() && currentIs(TextQueryExpressionType.WHITESPACE)) {
        currentIndex += 1;
      }
    }
  }

  public TextQueryToken current() {
    return this.tokens.get(currentIndex);
  }

  public int currentIndex() {
    return this.currentIndex;
  }

  public TextQueryToken atIndex(int index) {
    return tokens.get(index);
  }

  public boolean isSkippingWhiteSpaces() { return this.skipWhitespaces; }

  public void temporarilyIncludeWhitespaces() {
    this.whitespaceStateStack.push(this.skipWhitespaces);
    this.skipWhitespaces = false;
  }

  public void temporarilyExcludeWhitespaces() {
    this.whitespaceStateStack.push(this.skipWhitespaces);
    this.skipWhitespaces = true;
  }

  public void revertToPreviousWhitespaceInclusionState() {
    this.skipWhitespaces = this.whitespaceStateStack.pop();
  }

  public TextQueryToken readCurrentAndAdvance() {
    TextQueryToken current = this.current();
    this.advance();

    return current;
  }

  public <T extends TextQueryToken> T readCurrentAndAdvance(TextQueryExpressionType type) {
    if (currentIs(type)) {
      return (T) readCurrentAndAdvance();
    } else {
      throw new TextQueryException("%s Expected '%s', found '%s'", this.position(), type, this.current().getClass());
    }
  }

  public <T extends TextQueryToken> T readCurrentAndAdvanceMatchCase(TextQueryExpressionType type, String representation) {
    if (currentIsMatchCase(type, representation)) {
      return (T) readCurrentAndAdvance();
    } else {
      throw new TextQueryException("%s Expected ('%s' : %s), found ('%s' : %s)",
        this.position(),
        representation,
        type,
        this.current().toString(),
        this.current().getClass().getSimpleName());
    }
  }

  public <T extends TextQueryToken> T readCurrentAndAdvance(TextQueryExpressionType type, String representation) {
    if (currentIs(type, representation)) {
      return (T) readCurrentAndAdvance();
    } else {
      throw new TextQueryException("%s Expected ('%s' : %s), found ('%s' : %s)",
        this.position(),
        representation,
        type,
        this.current().toString(),
        this.current().getClass().getSimpleName());
    }
  }

  public boolean currentIs(TextQueryExpressionType type) {
    return !this.isEof() && this.current().type() == type;
  }

  public boolean currentIsMatchCase(TextQueryExpressionType type, String representation) {
    return currentIs(type) && current().toString().equals(representation);
  }

  public boolean currentIs(TextQueryExpressionType type, String representation) {
    return currentIs(type) && current().toString().equalsIgnoreCase(representation);
  }

  public boolean isEof() {
    return currentIndex >= this.tokens.size();
  }

  public void setUndoPoint() {
    this.undoStack.push(currentIndex);
  }

  public void rollbackToUndoPoint() {
    this.currentIndex = this.undoStack.pop();
  }

  public void clearUndoPoint() {
    this.undoStack.pop();
  }

  public ParquelLexPosition position() {
    return this.tokens.get(currentIndex).position();
  }
}
