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

import { ChangeListener, CreateReducedStateProps } from './types';

const createReducedState = <T>({ initialState, reducers, routines }: CreateReducedStateProps<T>) => {
  let state = initialState;
  let changeListeners: ChangeListener<T>[] = [];

  Object.entries(reducers).forEach(([action, reducer]) => {
    window.addEventListener(action, ((event: CustomEvent) => {
      const nextState = reducer(state, event.detail);
      state = nextState;
      changeListeners.forEach((changeListener) => changeListener(state));
    }) as EventListener);
  });

  Object.entries(routines).forEach(([action, routine]) => {
    window.addEventListener(action, ((event: CustomEvent) => {
      routine(state, event.detail);
    }) as EventListener);
  });

  const addChangeListener = (changeListenerToBeAdded: ChangeListener<T>) => {
    changeListeners = [...changeListeners, changeListenerToBeAdded];
  };

  const removeChangeListener = (changeListenerToBeRemoved: ChangeListener<T>) => {
    changeListeners = changeListeners.filter((changeListener) => changeListener !== changeListenerToBeRemoved);
  };

  return { state, addChangeListener, removeChangeListener };
};

export default createReducedState;
