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

import com.yscope.clp.compressorfrontend.AbstractClpEncodedSubquery;
import com.yscope.clp.compressorfrontend.BuiltInVariableHandlingRuleVersions;
import com.yscope.clp.compressorfrontend.ByteSegment;
import com.yscope.clp.compressorfrontend.EightByteClpEncodedSubquery;
import com.yscope.clp.compressorfrontend.EightByteClpWildcardQueryEncoder;
import java.io.File;
import java.io.IOException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.pinot.segment.local.segment.index.clp.ClpIndexType;
import org.apache.pinot.segment.local.segment.index.readers.text.LuceneTextIndexReader;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.segment.spi.index.reader.ClpIndexReader;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class ClpIndexReaderV1 implements ClpIndexReader {

  private static final WildcardToLuceneQueryEncoder _WILDCARD_TO_LUCENE_QUERY_ENCODER =
      new WildcardToLuceneQueryEncoder();
  private final LuceneTextIndexReader _logTypeLuceneIndexReader;
  private final LuceneTextIndexReader _dictVarLuceneIndexReader;
  private final LuceneTextIndexReader _encodedVarLuceneIndexReader;

  public ClpIndexReaderV1(File indexDir, int numDocs, String columnName)
      throws IOException {
    File clpIndexDir = new File(indexDir, columnName + ClpIndexType.EXTENSION);
    _logTypeLuceneIndexReader = new LuceneTextIndexReader(columnName + "_logtype_clp", clpIndexDir, numDocs,
        new TextIndexConfig(false, null, null, false, false, null, null, true, 500, null));
    _dictVarLuceneIndexReader = new LuceneTextIndexReader(columnName + "_dictvar_clp", clpIndexDir, numDocs,
        new TextIndexConfig(false, null, null, false, false, null, null, true, 500, null));
    _encodedVarLuceneIndexReader = new LuceneTextIndexReader(columnName + "_encodedvar_clp", clpIndexDir, numDocs,
        new TextIndexConfig(false, null, null, false, false, null, null, true, 500, null));
  }

  @Override
  public MutableRoaringBitmap getDocIds(String clpSearchQuery) {
    MutableRoaringBitmap result = new MutableRoaringBitmap();
    EightByteClpWildcardQueryEncoder queryEncoder =
        new EightByteClpWildcardQueryEncoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
            BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);
    EightByteClpEncodedSubquery[] subqueries = queryEncoder.encode(clpSearchQuery);

    for (EightByteClpEncodedSubquery query : subqueries) {
      result.or(evaluateQuery(query));
    }

    return result;
  }

  private MutableRoaringBitmap evaluateQuery(EightByteClpEncodedSubquery subquery) {
    MutableRoaringBitmap result =
        evaluateLogTypeQuery(subquery.getLogtypeQueryAsString(), subquery.logtypeQueryContainsWildcards());
    if (subquery.containsVariables()) {
      evaluateDictVarQuery(subquery, result);
      evaluateEncodedVarQuery(subquery, result);
    }
    return result;
  }

  private void evaluateEncodedVarQuery(EightByteClpEncodedSubquery subquery, MutableRoaringBitmap result) {
    for (long encodedVar : subquery.getEncodedVars()) {
      String textMatchQuery = "\"" + QueryParser.escape(String.valueOf(encodedVar)) + "\"";
      result.and(_encodedVarLuceneIndexReader.getDocIds(textMatchQuery));
    }
  }

  private void evaluateDictVarQuery(EightByteClpEncodedSubquery subquery, MutableRoaringBitmap result) {
    for (ByteSegment dictVar : subquery.getDictVars()) {
      String textMatchQuery = "\"" + QueryParser.escape(dictVar.toString()) + "\"";
      result.and(_dictVarLuceneIndexReader.getDocIds(textMatchQuery));
    }

    for (AbstractClpEncodedSubquery.VariableWildcardQuery wildcardQuery : subquery.getDictVarWildcardQueries()) {
      String luceneQuery;
      try {
        luceneQuery = _WILDCARD_TO_LUCENE_QUERY_ENCODER.encode(wildcardQuery.getQuery().toString());
      } catch (IOException e) {
        throw new RuntimeException("Failed to encode dictionary variable query into Lucene query.", e);
      }

      if (luceneQuery != null) {
        result.and(_dictVarLuceneIndexReader.getDocIds(luceneQuery));
      }
    }
  }

  private MutableRoaringBitmap evaluateLogTypeQuery(String query, boolean logtypeQueryContainsWildcards) {
    String luceneQuery;
    MutableRoaringBitmap result = new MutableRoaringBitmap();
    if (logtypeQueryContainsWildcards) {
      try {
        luceneQuery = _WILDCARD_TO_LUCENE_QUERY_ENCODER.encode(query);
      } catch (IOException e) {
        throw new RuntimeException("Failed to encode logtype query into a Lucene query.", e);
      }
    } else {
      luceneQuery = "\"" + QueryParser.escape(query) + "\"";
    }

    if (luceneQuery != null) {
      result.or(_logTypeLuceneIndexReader.getDocIds(luceneQuery));
    }

    return result;
  }

  @Override
  public void close()
      throws IOException {
    _logTypeLuceneIndexReader.close();
    _dictVarLuceneIndexReader.close();
    _encodedVarLuceneIndexReader.close();
  }
}
