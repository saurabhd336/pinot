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
package org.apache.pinot.segment.local.segment.creator.impl.clp;

import com.yscope.clp.compressorfrontend.BuiltInVariableHandlingRuleVersions;
import com.yscope.clp.compressorfrontend.EncodedMessage;
import com.yscope.clp.compressorfrontend.MessageEncoder;
import java.io.File;
import java.io.IOException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.segment.spi.index.creator.ClpIndexCreator;
import org.apache.pinot.spi.data.FieldSpec;


public class ClpIndexCreatorV1 implements ClpIndexCreator {
  private final EncodedMessage _clpEncodedMessage;
  private final MessageEncoder _clpMessageEncoder;
  private final LuceneTextIndexCreator _logTypeLuceneIndexCreator;
  private final LuceneTextIndexCreator _dictVarLuceneIndexCreator;
  private final LuceneTextIndexCreator _encodedVarLuceneIndexCreator;
  private final int _nextDocId = 0;

  public ClpIndexCreatorV1(File indexDir, String columnName)
      throws IOException {
    _clpEncodedMessage = new EncodedMessage();
    _clpMessageEncoder = new MessageEncoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
        BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);

    _logTypeLuceneIndexCreator = new LuceneTextIndexCreator(columnName + "_logtype_clp", indexDir, true,
        new TextIndexConfig(false, null, null, false, false, null, null, true, 500, null));
    _dictVarLuceneIndexCreator = new LuceneTextIndexCreator(columnName + "_dictvar_clp", indexDir, true,
        new TextIndexConfig(false, null, null, false, false, null, null, true, 500, null));
    _encodedVarLuceneIndexCreator = new LuceneTextIndexCreator(columnName + "_encodedvar_clp", indexDir, true,
        new TextIndexConfig(false, null, null, false, false, null, null, true, 500, null));
  }

  @Override
  public void add(@Nonnull Object value, int dictId)
      throws IOException {
    String logtype;
    String[] dictVars;
    Long[] encodedVars;

    try {
      _clpMessageEncoder.encodeMessage((String) value, _clpEncodedMessage);
      logtype = _clpEncodedMessage.getLogTypeAsString();
      dictVars = _clpEncodedMessage.getDictionaryVarsAsStrings();
      encodedVars = _clpEncodedMessage.getEncodedVarsAsBoxedLongs();
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to encode message: " + value, e);
    }

    if (logtype == null) {
      logtype = FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING;
    }

    if (dictVars == null) {
      dictVars = new String[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING};
    }

    if (encodedVars == null) {
      encodedVars = new Long[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG};
    }

    addCLPFields(logtype, dictVars, encodedVars);
  }

  private void addCLPFields(String logtype, String[] dictVars, Long[] encodedVars) {
    _logTypeLuceneIndexCreator.add(logtype);
    _dictVarLuceneIndexCreator.add(dictVars, dictVars.length);
    String[] encodedVarsStr = new String[encodedVars.length];
    for (int i = 0; i < encodedVars.length; i++) {
      encodedVarsStr[i] = encodedVars[i].toString();
    }
    _encodedVarLuceneIndexCreator.add(encodedVarsStr, encodedVarsStr.length);
  }

  @Override
  public void add(@Nonnull Object[] values, @Nullable int[] dictIds)
      throws IOException {
    throw new UnsupportedOperationException("clp only on SV raw encoded column");
  }

  @Override
  public void seal()
      throws IOException {
    _logTypeLuceneIndexCreator.seal();
    _dictVarLuceneIndexCreator.seal();
    _encodedVarLuceneIndexCreator.seal();
  }

  @Override
  public void close()
      throws IOException {
    _logTypeLuceneIndexCreator.close();
    _dictVarLuceneIndexCreator.close();
    _encodedVarLuceneIndexCreator.close();
  }
}
