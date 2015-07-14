package org.apache.parquet.parqour.query.lexing;

import org.apache.parquet.parqour.exceptions.TextQueryException;

import java.util.HashSet;
import java.util.Set;
import java.util.Stack;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class ParquelCharacterStream {
  private static final Set<Character> punctuation = generatePunctuationSet();

  private final String text;
  private int currentOffset;
  private int currentLine;
  private int currentColumn;
  private Stack<ParquelLexPosition> undoStack;

  public ParquelCharacterStream(String text) {
    this.text = text;
    this.currentOffset = 0;
    this.currentLine = 1;
    this.currentColumn = 1;

    this.undoStack = new Stack<ParquelLexPosition>();
  }

  public char current() {
    return text.charAt(currentOffset);
  }

  public boolean currentIs(char character) {
    return current() == character;
  }

  public char readCurrentAndAdvance() {
    char value = text.charAt(currentOffset);
    this.advance();
    return value;
  }

  public ParquelLexPosition position() {
    return new ParquelLexPosition(this.currentLine, this.currentColumn, this.currentOffset);
  }

  public void advance() {
    if (isEof()) {
      throw new TextQueryException("Attempt to advance past the end of file");
    }

    // If the last character was a newline, then increment the line number.
    if (text.charAt(currentOffset) == '\n') {
      this.currentLine += 1;
      this.currentColumn = 0;
    }

    // Advance the cursor
    this.currentOffset += 1;
    this.currentColumn += 1;
  }

  public boolean currentIsNumeric() {
    return Character.isDigit(current());
  }

  public boolean currentIsWhitespace() {
    return Character.isWhitespace(current());
  }

  public boolean currentIsAlpha() {
    return Character.isAlphabetic(current());
  }

  public boolean currentIsAlphaNumeric() {
    return Character.isLetterOrDigit(current());
  }

  public boolean currentIsPunctuation() {
    return punctuation.contains(current());
  }

  public boolean isEof() {
    return currentOffset >= text.length();
  }

  public void setUndoPoint() {
    this.undoStack.push(position());
  }

  public void rollbackToUndoPoint() {
    ParquelLexPosition currentPosition = this.undoStack.pop();

    this.currentOffset = currentPosition.offset();
    this.currentLine = currentPosition.line();
    this.currentColumn = currentPosition.column();
  }

  public void clearUndoPoint() {
    this.undoStack.pop();
  }

  private static Set<Character> generatePunctuationSet() {
    String punctuation = "~`!@#$%^&*()-_+=[{]}\\|;:,<.>/?'\"";
    Set<Character> punctuationSet = new HashSet<Character>();

    for (char letter : punctuation.toCharArray()) {
      punctuationSet.add(letter);
    }

    return punctuationSet;
  }
}
