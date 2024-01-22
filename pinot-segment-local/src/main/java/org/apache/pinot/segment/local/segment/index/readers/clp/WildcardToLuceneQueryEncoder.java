/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.segment.index.readers.clp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.queryparser.classic.QueryParser;


public class WildcardToLuceneQueryEncoder implements AutoCloseable {
  private final StandardAnalyzer _luceneAnalyzer = new StandardAnalyzer();

  private static boolean isWildcard(char c) {
    return '*' == c || '?' == c;
  }

  private static int findWildcardGroupAdjacentBeforeIdx(String value, int searchBeginIdx, int searchEndIdx) {
    boolean escaped = false;
    int beginIdx = -1;
    for (int i = searchBeginIdx; i < searchEndIdx; i++) {
      char c = value.charAt(i);

      if (escaped) {
        escaped = false;
      } else if ('\\' == c) {
        escaped = true;

        beginIdx = -1;
      } else if (isWildcard(c)) {
        if (-1 == beginIdx) {
          beginIdx = i;
        }
      } else {
        beginIdx = -1;
      }
    }

    return -1 == beginIdx ? searchEndIdx : beginIdx;
  }

  public static int findWildcardGroupAdjacentAfterIdx(String value, int searchBeginIdx, int searchEndIdx) {
    int endIdx = -1;
    for (int i = searchBeginIdx; i < searchEndIdx; i++) {
      char c = value.charAt(i);
      if (isWildcard(c)) {
        endIdx = i + 1;
      } else {
        break;
      }
    }

    return -1 == endIdx ? searchBeginIdx : endIdx;
  }

  public String encode(String wildcardQuery)
      throws IOException {
    // Get tokens by running Analyzer on query
    List<Token> tokens = new ArrayList<>();
    try (TokenStream tokenStream = _luceneAnalyzer.tokenStream("", wildcardQuery)) {
      OffsetAttribute offsetAttr = tokenStream.addAttribute(OffsetAttribute.class);

      tokenStream.reset();
      while (tokenStream.incrementToken()) {
        tokens.add(new Token(wildcardQuery, offsetAttr.startOffset(), offsetAttr.endOffset()));
      }
      tokenStream.end();
    }
    if (tokens.isEmpty()) {
      return null;
    }

    // Extend tokens to include wildcards
    int queryIdx = 0;
    int tokenWithWildcardsBeginIdx = -1;
    int tokenWithWildcardsEndIdx = -1;
    List<LuceneQueryToken> tokensWithWildcards = new ArrayList<>();
    boolean containsWildcards = false;
    for (int tokenIdx = 0; tokenIdx < tokens.size(); tokenIdx++) {
      Token token = tokens.get(tokenIdx);
      int tokenBeginIdx = token.getBeginIdx();
      int tokenEndIdx = token.getEndIdx();

      if (tokenWithWildcardsEndIdx != tokenBeginIdx) {
        if (-1 != tokenWithWildcardsBeginIdx) {
          tokensWithWildcards.add(
              new LuceneQueryToken(wildcardQuery, tokenWithWildcardsBeginIdx, tokenWithWildcardsEndIdx,
                  containsWildcards));
          containsWildcards = false;
        }

        tokenWithWildcardsBeginIdx = findWildcardGroupAdjacentBeforeIdx(wildcardQuery, queryIdx, tokenBeginIdx);
        if (tokenBeginIdx != tokenWithWildcardsBeginIdx) {
          containsWildcards = true;
        }
      }

      int nextTokenBeginIdx;
      if (tokenIdx + 1 < tokens.size()) {
        nextTokenBeginIdx = tokens.get(tokenIdx + 1).getBeginIdx();
      } else {
        nextTokenBeginIdx = wildcardQuery.length();
      }
      tokenWithWildcardsEndIdx = findWildcardGroupAdjacentAfterIdx(wildcardQuery, tokenEndIdx, nextTokenBeginIdx);
      if (tokenEndIdx != tokenWithWildcardsEndIdx) {
        containsWildcards = true;
      }

      queryIdx = tokenWithWildcardsEndIdx;
    }
    tokensWithWildcards.add(
        new LuceneQueryToken(wildcardQuery, tokenWithWildcardsBeginIdx, tokenWithWildcardsEndIdx, containsWildcards));

    // Encode the query
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < tokensWithWildcards.size(); i++) {
      LuceneQueryToken token = tokensWithWildcards.get(i);

      if (i > 0) {
        stringBuilder.append(" AND ");
      }

      if (token.containsWildcards()) {
        token.encodeIntoLuceneRegex(stringBuilder);
      } else {
        token.encodeIntoLuceneQuery(stringBuilder);
      }
    }
    return stringBuilder.toString();
  }

  @Override
  public void close() {
    _luceneAnalyzer.close();
  }

  static class Token {
    protected final int _beginIdx;
    protected final int _endIdx;

    protected final String _value;

    public Token(String value, int beginIdx, int endIdx) {
      _value = value;
      _beginIdx = beginIdx;
      _endIdx = endIdx;
    }

    public int getBeginIdx() {
      return _beginIdx;
    }

    public int getEndIdx() {
      return _endIdx;
    }
  }

  class LuceneQueryToken extends Token {
    private final char[] _luceneReservedChars = {
        '+', '-', '&', '|', '(', ')', '{', '}', '[', ']', '^', '"', '~', '\\', '<', '>', '.'
    };

    private final boolean _containsWildcards;

    LuceneQueryToken(String query, int beginIdx, int endIdx, boolean containsWildcards) {
      super(query, beginIdx, endIdx);
      _containsWildcards = containsWildcards;
    }

    public boolean containsWildcards() {
      return _containsWildcards;
    }

    public void encodeIntoLuceneRegex(StringBuilder stringBuilder) {
      stringBuilder.append('/');

      int unCopiedIdx = _beginIdx;
      boolean escaped = false;
      for (int queryIdx = _beginIdx; queryIdx < _endIdx; queryIdx++) {
        char queryChar = _value.charAt(queryIdx);

        if (!escaped && isWildcard(queryChar)) {
          stringBuilder.append(_value, unCopiedIdx, queryIdx);
          stringBuilder.append('.');
          unCopiedIdx = queryIdx;
        } else {
          for (int i = 0; i < _luceneReservedChars.length; i++) {
            if (queryChar == _luceneReservedChars[i]) {
              stringBuilder.append(_value, unCopiedIdx, queryIdx);
              stringBuilder.append('\\');
              unCopiedIdx = queryIdx;
              break;
            }
          }
        }
      }
      if (unCopiedIdx < _endIdx) {
        stringBuilder.append(_value, unCopiedIdx, _endIdx);
      }

      stringBuilder.append('/');
    }

    public void encodeIntoLuceneQuery(StringBuilder stringBuilder) {
      stringBuilder.append(QueryParser.escape(_value.substring(_beginIdx, _endIdx)));
    }
  }
}
