package org.apache.parquet.parqour.query.tokenization;

import org.apache.parquet.parqour.query.lexing.ParquelTokenList;
import org.apache.parquet.parqour.query.tokens.*;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by sircodesalot on 15/4/2.
 */
public class TestParquelTokenList {
  @Test
  public void testTokenization() {
    Map<Class, Integer> tokensCounts = new HashMap<Class, Integer>();
    ParquelTokenList tokens = new ParquelTokenList("select 1, second.third from table1, where name = 'smith'");

    for (ParquelToken token : tokens) {
      Class tokenType = token.getClass();
      if (tokensCounts.containsKey(token.getClass())) {
        int seenTimes = tokensCounts.get(tokenType);
        tokensCounts.put(tokenType, seenTimes + 1);
      } else {
        tokensCounts.put(tokenType, 1);
      }
    }

    assert (tokens.size() == 23);

    assert (tokensCounts.get(ParquelWhitespaceToken.class) == 8);
    assert (tokensCounts.get(ParquelIdentifierToken.class) == 8);
    assert (tokensCounts.get(ParquelNumericToken.class) == 1);
    assert (tokensCounts.get(ParquelPunctuationToken.class) == 6);
  }
}
