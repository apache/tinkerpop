/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tinkerpop.gremlin.server.notifications;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.util.DefaultGraphManager;

public class ProviderGraphManagerHelper extends DefaultGraphManager {

  private Map<String, Map<String, Object>> beforeQueryStartTracking;

  private List<String> onQuerySuccessCount;

  private List<String> onQueryErrorCount;

  public ProviderGraphManagerHelper(final Settings settings) {
    super(settings);
    this.beforeQueryStartTracking = new HashMap<String, Map<String, Object>>();
    this.onQuerySuccessCount = new ArrayList<String>();
    this.onQueryErrorCount = new ArrayList<String>();
  }
 
  @Override
  public void beforeQueryStart(final RequestMessage msg) {
    final Map<String, Object> args = msg.getArgs();
    final String requestID = msg.getRequestId().toString();
    this.beforeQueryStartTracking.put(requestID, args);
  }

  @Override
  public void onQuerySuccess(final RequestMessage msg) {
    final String requestID = msg.getRequestId().toString();
    this.onQuerySuccessCount.add(requestID);
  }

  @Override
  public void onQueryError(final RequestMessage msg, final Throwable e) {
    final String requestID = msg.getRequestId().toString();
    this.onQueryErrorCount.add(requestID);
  }

  public Map<String, Object> getBeforeQueryStartTracking(final String requestID) {
    return this.beforeQueryStartTracking.get(requestID);
  }

  public boolean didRequestSucceed(final String requestID) {
    return this.onQuerySuccessCount.contains(requestID);
  }

  public boolean didRequestFail(final String requestID) {
    return this.onQueryErrorCount.contains(requestID);
  }

}
