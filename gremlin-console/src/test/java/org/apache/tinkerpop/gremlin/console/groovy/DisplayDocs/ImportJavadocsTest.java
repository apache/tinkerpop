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
package org.apache.tinkerpop.gremlin.console.groovy.DisplayDocs;

/**
 *
 * @author xristosoik (https://github.com/xristosoik)
 */

import static org.junit.Assert.*;
import org.junit.Test;

public class ImportJavadocsTest {
    @Test
    public void behaviorFindPath2CorrectLineWeb () throws Exception {
        String[] url = ImportJavadocs.findPath2CorrectLineWeb(">" + "Graph" + "<");
        assertTrue(url[0].contains("http://tinkerpop.incubator.apache.org/javadocs"));
        assertTrue(url[0].contains("core"));
        assertTrue(url[1].contains("org/apache/tinkerpop/gremlin/structure/Graph.html"));
    }
    
    @Test
    public void behaviorFindPath2CorrectLineWebATNotExistanceSituation () throws Exception {
        String[] url = ImportJavadocs.findPath2CorrectLineWeb(">" + "something" + "<");
        
        assertFalse(url[0] == null);
        assertFalse(url[1] == null);
    }
    
    @Test
    public void behaviorOfFindCorrectPath2Javadocs () throws Exception {
        String url = ImportJavadocs.findCorrectPath2Javadocs ();
        assertNotNull(url);
    }
    
    @Test
    public void behaviorOfFindPath2CorrectLineATNotExistanceSituation () throws Exception {
        String[] path = ImportJavadocs.findPath2CorrectLineWeb(">" + "something" + "<");
        assertNotNull(path[0]);
        assertNotNull(path[1]);
    }
    
    @Test
    public void behaviorOfFindClass() throws Exception {
        DocStructure doc = ImportJavadocs.findClass("Graph");
        assertNotNull(doc);
        assertTrue(doc.getMethodList().size() > 0);
    }
    
    @Test
    public void behaviorOfFindClassNotExistanceSituation() throws Exception {
        DocStructure doc = ImportJavadocs.findClass("Graph");
        doc = ImportJavadocs.findClass("Something");
        assertNotNull(doc);
        assertTrue(doc.getMethodList().size() > 0);
    }
}
