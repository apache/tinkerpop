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

import React from 'react';
import styled from 'styled-components';
import { inputTextColor } from '../styleVariables';

const QueryInputWrapper = styled.div`
  padding: 10px;
`;

const QueryInputTextArea = styled.textarea`
  height: calc(100vh / 4);
  border-radius: 5px;
  font-family: 'Courier New', Courier, monospace;
  background: rgba(0, 0, 0, 0.05);
  outline: none;
  font-size: 16px;
  padding: 10px;
  border: none;
  resize: none;
  width: 100%;
  box-shadow: inset rgba(0, 0, 0, 0.5) 0 0 10px -5px;
  color: ${inputTextColor};
  box-sizing: border-box;
`;

type QueryInputProps = {
  onChange?: ((event: React.ChangeEvent<HTMLTextAreaElement>) => void) | undefined;
  value: string;
};

const QueryInput = ({ onChange, value }: QueryInputProps) => (
  <QueryInputWrapper>
    <QueryInputTextArea onChange={onChange} value={value} rows={25} />
  </QueryInputWrapper>
);

export default QueryInput;
