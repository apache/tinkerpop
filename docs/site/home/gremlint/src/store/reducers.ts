/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { formatQuery } from 'gremlint';
import {
  FORMAT_QUERY,
  SET_INDENTATION,
  SET_MAX_LINE_LENGTH,
  SET_QUERY_INPUT,
  SET_SHOULD_PLACE_DOTS_AFTER_LINE_BREAKS,
  TOGGLE_SHOULD_SHOW_ADVANCED_OPTIONS,
} from './actions';
import { State } from './types';

const handleSetQueryInput = (state: State, queryInput: string) => ({
  ...state,
  queryInput,
});

const handleFormatQuery = (state: State) => ({
  ...state,
  queryOutput: formatQuery(state.queryInput, {
    indentation: state.indentation,
    maxLineLength: state.maxLineLength,
    shouldPlaceDotsAfterLineBreaks: state.shouldPlaceDotsAfterLineBreaks,
  }),
});

const handleToggleShouldShowAdvancedOptions = (state: State) => ({
  ...state,
  shouldShowAdvancedOptions: !state.shouldShowAdvancedOptions,
});

const handleSetIndentation = (state: State, unparsedIndentation: string) => {
  const indentation = parseInt(unparsedIndentation);
  if (isNaN(indentation)) return { ...state };
  if (indentation < 0) return { ...state, indentation: 0 };
  const { maxLineLength } = state;
  if (indentation > maxLineLength) {
    return { ...state, indentation: maxLineLength };
  }
  return { ...state, indentation };
};

const handleSetMaxLineLength = (state: State, unparsedMaxLineLength: string) => {
  const maxLineLength = parseInt(unparsedMaxLineLength);
  if (isNaN(maxLineLength)) return { ...state };
  const { indentation } = state;
  if (maxLineLength < indentation) {
    return { ...state, maxLineLength: indentation };
  }
  return { ...state, maxLineLength };
};

const handleSetShouldPlaceDotsAfterLineBreaks = (state: State, shouldPlaceDotsAfterLineBreaks: boolean) => ({
  ...state,
  shouldPlaceDotsAfterLineBreaks,
});

const reducers = {
  [SET_QUERY_INPUT]: handleSetQueryInput,
  [FORMAT_QUERY]: handleFormatQuery,
  [TOGGLE_SHOULD_SHOW_ADVANCED_OPTIONS]: handleToggleShouldShowAdvancedOptions,
  [SET_INDENTATION]: handleSetIndentation,
  [SET_MAX_LINE_LENGTH]: handleSetMaxLineLength,
  [SET_SHOULD_PLACE_DOTS_AFTER_LINE_BREAKS]: handleSetShouldPlaceDotsAfterLineBreaks,
};

export default reducers;
