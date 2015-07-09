package org.apache.parquet.parqour.query.lexing;


import org.apache.parquet.parqour.exceptions.ParquelException;
import org.apache.parquet.parqour.query.expressions.categories.ParquelExpressionType;
import org.apache.parquet.parqour.query.tokens.ParquelToken;

import java.util.Stack;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class ParquelLexer {
  private final String text;
  private final ParquelTokenList tokens;
  private boolean skipWhitespaces;
  private int currentIndex;
  private Stack<Integer> undoStack;
  private Stack<Boolean> whitespaceStateStack;

  public ParquelLexer(String text, boolean skipWhitespacesByDefault) {
    this.text = text;
    this.skipWhitespaces = skipWhitespacesByDefault;
    this.tokens = new ParquelTokenList(text);
    this.undoStack = new Stack<Integer>();
    this.whitespaceStateStack = new Stack<Boolean>();
  }

  public String text() {
    return this.text;
  }

  public void advance() {
    if (isEof()) {
      throw new ParquelException("Attempt to advance past end of file");
    }

    currentIndex += 1;

    if (skipWhitespaces) {
      while (!isEof() && currentIs(ParquelExpressionType.WHITESPACE)) {
        currentIndex += 1;
      }
    }
  }

  public ParquelToken current() {
    return this.tokens.get(currentIndex);
  }

  public int currentIndex() {
    return this.currentIndex;
  }

  public ParquelToken atIndex(int index) {
    return tokens.get(index);
  }

  public boolean isIncludingWhitespaces() {
    return this.skipWhitespaces;
  }

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

  public ParquelToken readCurrentAndAdvance() {
    ParquelToken current = this.current();
    this.advance();

    return current;
  }

  public <T extends ParquelToken> T readCurrentAndAdvance(ParquelExpressionType type) {
    if (currentIs(type)) {
      return (T) readCurrentAndAdvance();
    } else {
      throw new ParquelException("%s Expected '%s', found '%s'", this.position(), type, this.current().getClass());
    }
  }

  public <T extends ParquelToken> T readCurrentAndAdvanceMatchCase(ParquelExpressionType type, String representation) {
    if (currentIsMatchCase(type, representation)) {
      return (T) readCurrentAndAdvance();
    } else {
      throw new ParquelException("%s Expected ('%s' : %s), found ('%s' : %s)",
        this.position(),
        representation,
        type,
        this.current().toString(),
        this.current().getClass().getSimpleName());
    }
  }

  public <T extends ParquelToken> T readCurrentAndAdvance(ParquelExpressionType type, String representation) {
    if (currentIs(type, representation)) {
      return (T) readCurrentAndAdvance();
    } else {
      throw new ParquelException("%s Expected ('%s' : %s), found ('%s' : %s)",
        this.position(),
        representation,
        type,
        this.current().toString(),
        this.current().getClass().getSimpleName());
    }
  }

  public boolean currentIs(ParquelExpressionType type) {
    return !this.isEof() && this.current().type() == type;
  }

  public boolean currentIsMatchCase(ParquelExpressionType type, String representation) {
    return currentIs(type) && current().toString().equals(representation);
  }

  public boolean currentIs(ParquelExpressionType type, String representation) {
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
