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

import org.apache.tinkerpop.gremlin.server.GremlinServer
import org.apache.tinkerpop.gremlin.server.KdcFixture
import org.apache.tinkerpop.gremlin.server.Settings

////////////////////////////////////////////////////////////////////////////////
// IMPORTANT
////////////////////////////////////////////////////////////////////////////////
// Changes to this file need to be appropriately replicated to
//
// - docker/gremlin-server/*
// - docker/gremlin-server.sh
//
// Without such changes, the test docker server can't be started for independent
// testing with Gremlin Language Variants.
////////////////////////////////////////////////////////////////////////////////

if (Boolean.parseBoolean(skipTests)) return

log.info("Starting Gremlin Server instances for native testing of ${executionName}")

def platformAgnosticBaseDirPath = "${tinkerpopRootDir}".replace("\\", "/")
def platformAgnosticGremlinServerDir = platformAgnosticBaseDirPath + "/gremlin-server"
def platformAgnosticSettingsFile = platformAgnosticGremlinServerDir + "/src/test/resources/org" +
        "/apache/tinkerpop/gremlin/server/gremlin-server-integration.yaml"

def settings = Settings.read("${platformAgnosticSettingsFile}")
settings.graphs.graph = platformAgnosticGremlinServerDir + "/src/test/scripts/tinkergraph-empty.properties"
settings.graphs.classic = platformAgnosticGremlinServerDir + "/src/test/scripts/tinkergraph-empty.properties"
settings.graphs.modern = platformAgnosticGremlinServerDir + "/src/test/scripts/tinkergraph-empty.properties"
settings.graphs.crew = platformAgnosticGremlinServerDir + "/src/test/scripts/tinkergraph-empty.properties"
settings.graphs.grateful = platformAgnosticGremlinServerDir + "/src/test/scripts/tinkergraph-empty.properties"
settings.graphs.sink = platformAgnosticGremlinServerDir + "/src/test/scripts/tinkergraph-empty.properties"
settings.graphs.tx = platformAgnosticGremlinServerDir + "/src/test/scripts/tinkertransactiongraph-empty.properties"
settings.scriptEngines["gremlin-groovy"].plugins["org.apache.tinkerpop.gremlin.jsr223.ScriptFileGremlinPlugin"].files = [platformAgnosticGremlinServerDir + "/src/test/scripts/generate-all.groovy"]
settings.port = 45940

def server = new GremlinServer(settings)
server.start().join()

project.setContextValue("gremlin.server", server)
log.info("Gremlin Server without authentication started on port 45940")


def securePropsFile = new File("${platformAgnosticBaseDirPath}/target/tinkergraph-credentials.properties")
if (!securePropsFile.exists()) {
    securePropsFile.createNewFile()
    securePropsFile << "gremlin.graph=org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph\n"
    securePropsFile << "gremlin.tinkergraph.vertexIdManager=LONG\n"
    securePropsFile << "gremlin.tinkergraph.graphLocation=${platformAgnosticGremlinServerDir}/data/credentials.kryo\n"
    securePropsFile << "gremlin.tinkergraph.graphFormat=gryo"
}

def settingsSecure = Settings.read("${platformAgnosticSettingsFile}")
settingsSecure.graphs.graph = platformAgnosticGremlinServerDir + "/src/test/scripts/tinkergraph-empty.properties"
settingsSecure.graphs.classic = platformAgnosticGremlinServerDir + "/src/test/scripts/tinkergraph-empty.properties"
settingsSecure.graphs.modern = platformAgnosticGremlinServerDir + "/src/test/scripts/tinkergraph-empty.properties"
settingsSecure.graphs.crew = platformAgnosticGremlinServerDir + "/src/test/scripts/tinkergraph-empty.properties"
settingsSecure.graphs.grateful = platformAgnosticGremlinServerDir + "/src/test/scripts/tinkergraph-empty.properties"
settingsSecure.graphs.sink = platformAgnosticGremlinServerDir + "/src/test/scripts/tinkergraph-empty.properties"
settingsSecure.scriptEngines["gremlin-groovy"].plugins["org.apache.tinkerpop.gremlin.jsr223.ScriptFileGremlinPlugin"].files = [platformAgnosticGremlinServerDir + "/src/test/scripts/generate-all.groovy"]
settingsSecure.port = 45941
settingsSecure.authentication.authenticator = "org.apache.tinkerpop.gremlin.server.auth.SimpleAuthenticator"
settingsSecure.authentication.config = [credentialsDb: platformAgnosticBaseDirPath + "/target/tinkergraph-credentials.properties"]
settingsSecure.ssl = new Settings.SslSettings()
settingsSecure.ssl.enabled = true
settingsSecure.ssl.sslEnabledProtocols = ["TLSv1.2"]
settingsSecure.ssl.keyStore = platformAgnosticGremlinServerDir + "/src/test/resources/server-key.jks"
settingsSecure.ssl.keyStorePassword = "changeit"

def serverSecure = new GremlinServer(settingsSecure)
serverSecure.start().join()

project.setContextValue("gremlin.server.secure", serverSecure)
log.info("Gremlin Server with password authentication started on port 45941")


def kdcServer = new KdcFixture(projectBaseDir)
kdcServer.setUp()

project.setContextValue("gremlin.server.kdcserver", kdcServer)
log.info("KDC started with configuration ${projectBaseDir}/target/kdc/krb5.conf")

def settingsKrb5 = Settings.read("${settingsFile}")
settingsKrb5.graphs.graph = gremlinServerDir + "/src/test/scripts/tinkergraph-empty.properties"
settingsKrb5.graphs.classic = gremlinServerDir + "/src/test/scripts/tinkergraph-empty.properties"
settingsKrb5.graphs.modern = gremlinServerDir + "/src/test/scripts/tinkergraph-empty.properties"
settingsKrb5.graphs.crew = gremlinServerDir + "/src/test/scripts/tinkergraph-empty.properties"
settingsKrb5.graphs.grateful = gremlinServerDir + "/src/test/scripts/tinkergraph-empty.properties"
settingsKrb5.graphs.sink = gremlinServerDir + "/src/test/scripts/tinkergraph-empty.properties"
settingsKrb5.scriptEngines["gremlin-groovy"].plugins["org.apache.tinkerpop.gremlin.jsr223.ScriptFileGremlinPlugin"].files = [gremlinServerDir + "/src/test/scripts/generate-all.groovy"]
settingsKrb5.port = 45942
settingsKrb5.authentication.authenticator = "org.apache.tinkerpop.gremlin.server.auth.Krb5Authenticator"
settingsKrb5.authentication.config = [
        "principal": kdcServer.serverPrincipal,
        "keytab": kdcServer.serviceKeytabFile.getAbsolutePath()]

def serverKrb5 = new GremlinServer(settingsKrb5)
serverKrb5.start().join()

project.setContextValue("gremlin.server.krb5", serverKrb5)
log.info("Gremlin Server with kerberos authentication started on port 45942")
