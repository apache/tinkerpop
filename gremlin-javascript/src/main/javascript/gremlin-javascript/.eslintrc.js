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

'use strict';

module.exports = {
  env: {
    commonjs: true,
    es6: true,
  },
  parserOptions: {
    ecmaVersion: 2017,
  },
  ignorePatterns: ['test/**/*.js', 'doc/**/*.js'],
  extends: ['eslint:recommended', 'prettier'],
  plugins: ['prettier'],
  rules: {
    'prettier/prettier': ['error'],
    indent: ['error', 2, { SwitchCase: 1 }],
    'linebreak-style': ['error', 'unix'],
    quotes: ['error', 'single'],
    semi: ['error', 'always'],
    'no-constant-condition': ['error', { checkLoops: false }],
    strict: ['error', 'global'],
    'array-callback-return': 'error',
    curly: 'error',
    'no-unused-vars': ['error', { args: 'none' }],
    'global-require': 'error',
    eqeqeq: ['error', 'allow-null'],

    // make sure for-in loops have an if statement
    'guard-for-in': 'error',
    'no-alert': 'error',
    'no-caller': 'error',
    'no-case-declarations': 'error',
    'no-else-return': 'error',
    'no-empty-pattern': 'error',
    'no-eval': 'error',
    'no-extend-native': 'error',
    'no-extra-bind': 'error',
    'no-extra-label': 'error',
    'no-floating-decimal': 'error',
    'no-global-assign': ['error', { exceptions: [] }],
    'no-implicit-coercion': [
      'off',
      {
        boolean: false,
        number: true,
        string: true,
        allow: [],
      },
    ],
    'no-implied-eval': 'error',
    'no-labels': ['error', { allowLoop: false, allowSwitch: false }],
    'no-lone-blocks': 'error',
    'no-loop-func': 'error',
    //
    // disallow use of multiple spaces
    'no-multi-spaces': 'error',
    'no-new': 'error',
    'no-new-func': 'error',
    'no-new-wrappers': 'error',
    'no-octal-escape': 'error',
    'no-proto': 'error',
    'no-prototype-builtins': 0,
    'no-redeclare': 'error',
    'no-restricted-properties': [
      'error',
      {
        object: 'arguments',
        property: 'callee',
        message: 'arguments.callee is deprecated',
      },
      {
        property: '__defineGetter__',
        message: 'Please use Object.defineProperty instead.',
      },
      {
        property: '__defineSetter__',
        message: 'Please use Object.defineProperty instead.',
      },
    ],
    'no-self-assign': 'error',
    'no-self-compare': 'error',
    'no-sequences': 'error',
    'no-throw-literal': 'error',
    'no-unmodified-loop-condition': 'off',
    'no-unused-expressions': [
      'error',
      {
        allowShortCircuit: false,
        allowTernary: false,
      },
    ],
    'no-useless-call': 'off',
    'no-useless-concat': 'error',
    'no-useless-escape': 'error',
    'no-useless-return': 'error',
    'no-void': 'error',
    'no-with': 'error',
    'no-buffer-constructor': 'error',
    radix: 'error',
    'no-var': 'error',
    'prefer-const': 'error',
    'arrow-body-style': ['error', 'as-needed'],
    'arrow-spacing': 'error',
    'no-confusing-arrow': ['error', { allowParens: true }],
    yoda: 'error',
    'constructor-super': 'error',
    'require-await': 'error',
    'require-atomic-updates': 'off',
  },
  globals: {
    Buffer: false,
    Promise: true,
    Symbol: false,
    Uint16Array: false,
    Int32Array: false,
    Int8Array: false,
    BigInt: false,
    process: false,
    setInterval: false,
    setTimeout: false,
    setImmediate: false,
    clearInterval: false,
    clearTimeout: false,
    describe: false,
    xdescribe: false,
    it: false,
    xit: false,
    context: false,
    after: false,
    afterEach: false,
    before: false,
    beforeEach: false,
    __filename: false,
  },
};
