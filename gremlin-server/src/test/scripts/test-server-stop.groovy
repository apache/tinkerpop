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

if (Boolean.parseBoolean(skipTests)) return

log.info("Tests for native ${executionName} complete")

def server = project.getContextValue("gremlin.server")
log.info("Shutting down $server")
server.stop().join()

def serverSecure = project.getContextValue("gremlin.server.secure")
log.info("Shutting down $serverSecure")
serverSecure.stop().join()

def kdcServer = project.getContextValue("gremlin.server.kdcserver")
log.info("Shutting down $kdcServer")
kdcServer.close()

def serverKrb5 = project.getContextValue("gremlin.server.krb5")
log.info("Shutting down $serverKrb5")
serverKrb5.stop().join()

log.info("All Gremlin Server instances are shutdown for ${executionName}")