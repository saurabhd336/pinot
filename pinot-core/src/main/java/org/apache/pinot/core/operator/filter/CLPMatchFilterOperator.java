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
package org.apache.pinot.core.operator.filter;

import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.predicate.RegexpLikePredicate;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.segment.spi.index.reader.ClpIndexReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

public class CLPMatchFilterOperator extends BaseFilterOperator {
  private static final String EXPLAIN_NAME = "CLP_INDEX_FILTER_OPERATOR";
  private ImmutableRoaringBitmap _matches;
  private final ClpIndexReader _clpIndexReader;
  private final RegexpLikePredicate _predicate;
  public CLPMatchFilterOperator(ClpIndexReader clpIndexReader, RegexpLikePredicate predicate, int numDocs) {
    super(numDocs, false);
    _clpIndexReader = clpIndexReader;
    _predicate = predicate;
  }

  @Override
  public List<? extends Operator> getChildOperators() {
    return Collections.emptyList();
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected BlockDocIdSet getTrues() {
    if (_matches == null) {
      _matches = _clpIndexReader.getDocIds(_predicate.getValue());
    }
    return new BitmapDocIdSet(_matches, _numDocs);
  }
}
