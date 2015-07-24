package org.apache.parquet.parqour.query.lexing;


import org.apache.parquet.parqour.query.tokens.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class ParquelTokenList implements Iterable<TextQueryToken> {
  private final List<TextQueryToken> tokens;

  public ParquelTokenList(String statement) {
    this.tokens = generateFromString(statement);
  }

  private List<TextQueryToken> generateFromString(String text) {
    ParquelCharacterStream stream = new ParquelCharacterStream(text);
    List<TextQueryToken> tokens = new ArrayList<TextQueryToken>();

    while (!stream.isEof()) {
      TextQueryToken token = readToken(stream);
      tokens.add(token);
    }

    return tokens;
  }

  private TextQueryToken readToken(ParquelCharacterStream stream) {
    if (stream.currentIsAlpha()) {
      return TextQueryIdentifierToken.read(stream);
    } else if (stream.currentIsPunctuation()) {
      return TextQueryPunctuationToken.read(stream);
    } else if (stream.currentIsWhitespace()) {
      return TextQueryWhitespaceToken.read(stream);
    } else if (stream.currentIsPunctuation()) {
      return TextQueryPunctuationToken.read(stream);
    } else if (stream.currentIsNumeric()) {
      // TODO: Add support for negative and decimal numbers.
      return TextQueryNumericToken.read(stream);
    } else {
      // In practice, this should never be used.
      return TextQueryMysteryToken.read(stream);
    }
  }

  public Iterable<TextQueryToken> tokens() {
    return this.tokens;
  }

  public Iterator<TextQueryToken> iterator() {
    return tokens.iterator();
  }

  public int size() {
    return tokens.size();
  }

  public TextQueryToken get(int index) {
    return tokens.get(index);
  }
}
