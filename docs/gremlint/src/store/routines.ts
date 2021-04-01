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

import { dispatch } from '../libs/reduced-state';
import {
  SET_QUERY_INPUT,
  FORMAT_QUERY,
  SET_INDENTATION,
  SET_MAX_LINE_LENGTH,
  SET_SHOULD_PLACE_DOTS_AFTER_LINE_BREAKS,
} from './actions';

const handleSetQueryInput = () => dispatch(FORMAT_QUERY);
const handleSetIndentation = () => dispatch(FORMAT_QUERY);
const handleSetMaxLineLength = () => dispatch(FORMAT_QUERY);
const handleSetShouldPlaceDotsAfterLineBreaks = () => dispatch(FORMAT_QUERY);

const routines = {
  [SET_QUERY_INPUT]: handleSetQueryInput,
  [SET_INDENTATION]: handleSetIndentation,
  [SET_MAX_LINE_LENGTH]: handleSetMaxLineLength,
  [SET_SHOULD_PLACE_DOTS_AFTER_LINE_BREAKS]: handleSetShouldPlaceDotsAfterLineBreaks,
};

export default routines;
