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
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.dociditerators.ScanBasedDocIdIterator;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.core.operator.filter.predicate.RegexpLikePredicateEvaluatorFactory;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.reader.ClpIndexReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class CLPMatchFilterOperator extends BaseColumnFilterOperator {
  private static final String EXPLAIN_NAME = "CLP_INDEX_FILTER_OPERATOR";
  private ImmutableRoaringBitmap _matches;
  private final ClpIndexReader _clpIndexReader;
  private final RegexpLikePredicateEvaluatorFactory.RawValueBasedRegexpLikePredicateEvaluator _predicateEvaluator;

  public CLPMatchFilterOperator(QueryContext queryContext, DataSource dataSource,
      RegexpLikePredicateEvaluatorFactory.RawValueBasedRegexpLikePredicateEvaluator predicateEvaluator, int numDocs) {
    super(queryContext, dataSource, numDocs);
    _clpIndexReader = dataSource.getIndex(StandardIndexes.clp());
    _predicateEvaluator = predicateEvaluator;
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
  protected BlockDocIdSet getNextBlockWithoutNullHandling() {
    if (_matches == null) {
      _matches = _clpIndexReader.getDocIds(_predicateEvaluator.getQuery());
    }

    ScanBasedFilterOperator scanBasedFilterOperator =
        new ScanBasedFilterOperator(_queryContext, _predicateEvaluator, _dataSource, _numDocs);
    BlockDocIdSet scanBasedDocIdSet = scanBasedFilterOperator.getTrues();
    MutableRoaringBitmap docIds = ((ScanBasedDocIdIterator) scanBasedDocIdSet.iterator()).applyAnd(_matches);
    return new BitmapDocIdSet(docIds, _numDocs) {
      // Override this method to reflect the entries scanned
      @Override
      public long getNumEntriesScannedInFilter() {
        return scanBasedDocIdSet.getNumEntriesScannedInFilter();
      }
    };
  }
}
