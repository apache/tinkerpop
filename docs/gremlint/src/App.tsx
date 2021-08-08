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

import React, { useState } from 'react';
import './App.css';
import styled from 'styled-components';
import { useRouter } from 'sharp-router';
import router from './router';
import FadeIn from './components/FadeIn';
import Navigator from './components/Navigator';
import View from './components/View';
import Footer from './components/Footer';
import LoadingAnimation from './components/LoadingAnimation';

const AppWrapper = styled.div`
  display: flex;
  flex-direction: column;
  height: 100vh;
`;

const App = () => {
  const { matchedRoute } = useRouter(router);
  const [loadingComplete, setLoadingComplete] = useState(false);
  if (!loadingComplete) return <LoadingAnimation onLoadingComplete={() => setLoadingComplete(true)} />;
  return (
    <FadeIn>
      <AppWrapper>
        <Navigator matchedRoute={matchedRoute} />
        <View matchedRoute={matchedRoute} />
        <Footer />
      </AppWrapper>
    </FadeIn>
  );
};

export default App;
