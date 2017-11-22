/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

/**
 * @author Jorge Bay Gondra
 */
'use strict';

const {defineSupportCode} = require('cucumber');
const vm = require('vm');


defineSupportCode(function({Given, When, Then}) {
  Given(/^the (.+) graph$/, function (graphName) {
    //TODO: Set context g
  });
  // Given('the graph initializer of');
  Given('an unsupported test', () => {});

  Given('the traversal of', function (traversalText) {
    //TODO: make traversal
  });

  Given(/^$/, function (paramName, stringValue) {
    //TODO: Add parameter
  });

  When('iterated to list', function () {
    //TODO
  });

  When('iterated next', function () {
    //TODO
  });

  Then(/^the result should be (\w+)$/, function (characterizedAs, resultTable) {
    //TODO
    //console.log('--resultTable', resultTable.rows());
    if (typeof resultTable === 'function'){
      return resultTable();
    }
  });

  Then(/^the graph should return (\d+) for count of (.+)$/, function (stringCount, traversalString) {

  });

  Then(/^the result should have a count of (\d+)$/, function (stringCount) {

  });

  Then('nothing should happen because', () => {});
});