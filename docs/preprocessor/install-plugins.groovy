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

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
import org.apache.tinkerpop.gremlin.groovy.util.Artifact
import org.apache.tinkerpop.gremlin.groovy.util.DependencyGrabber

installPlugin = { def artifact ->
  def classLoader = new groovy.lang.GroovyClassLoader()
  def extensionPath = System.getProperty("user.dir") + System.getProperty("file.separator") + "ext"
  try {
    System.err.print(" * ${artifact.getArtifact()} ... ")
    new DependencyGrabber(classLoader, extensionPath).copyDependenciesToPath(artifact)
    System.err.println("done")
  } catch (Exception e) {
    System.err.println("failed")
    System.err.println()
    System.err.println(e.getMessage())
    e.printStackTrace()
    System.exit(1)
  }
}

:plugin use tinkerpop.sugar
:plugin use tinkerpop.credentials
System.err.println("done")
