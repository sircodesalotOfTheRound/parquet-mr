package org.apache.parquet.parqour.query.lexing;


import org.apache.parquet.parqour.query.tokens.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class ParquelTokenList implements Iterable<ParquelToken> {
  private final List<ParquelToken> tokens;

  public ParquelTokenList(String statement) {
    this.tokens = generateFromString(statement);
  }

  private List<ParquelToken> generateFromString(String text) {
    ParquelCharacterStream stream = new ParquelCharacterStream(text);
    List<ParquelToken> tokens = new ArrayList<ParquelToken>();

    while (!stream.isEof()) {
      ParquelToken token = readToken(stream);
      tokens.add(token);
    }

    return tokens;
  }

  private ParquelToken readToken(ParquelCharacterStream stream) {
    if (stream.currentIsAlpha()) {
      return ParquelIdentifierToken.read(stream);
    } else if (stream.currentIsPunctuation()) {
      return ParquelPunctuationToken.read(stream);
    } else if (stream.currentIsWhitespace()) {
      return ParquelWhitespaceToken.read(stream);
    } else if (stream.currentIsPunctuation()) {
      return ParquelPunctuationToken.read(stream);
    } else if (stream.currentIsNumeric()) {
      return ParquelNumericToken.read(stream);
    } else {
      // In practice, this should never be used.
      return ParquelMysteryToken.read(stream);
    }
  }

  public Iterable<ParquelToken> tokens() {
    return this.tokens;
  }

  public Iterator<ParquelToken> iterator() {
    return tokens.iterator();
  }

  public int size() {
    return tokens.size();
  }

  public ParquelToken get(int index) {
    return tokens.get(index);
  }
}
