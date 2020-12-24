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
package org.apache.tinkerpop.gremlin.server.authz;

import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;


/**
 * AllowList for the AllowListAuthorizer as configured by a YAML file.
 *
 * @author Marc de Lignie
 */
public class AllowList {

    /**
     * Holds lists of groups by grant. A grant is either a TraversalSource name or the "sandbox" value. With the
     * sandbox grant users can access all TraversalSource instances and execute groovy scripts as string based
     * requests or as lambda functions, only limited by Gremlin Server's sandbox definition.
     */
    public Map<String, List<String>> grants;

    /**
     * Holds lists of user names by groupname. The "anonymous" user name can be used to denote any user.
     */
    public Map<String, List<String>> groups;

    /**
     * Read a configuration from a YAML file into an {@link AllowList} object.
     *
     * @param file the location of a AllowList YAML configuration file
     * @return An {@link Optional} object wrapping the created {@link AllowList}
     */
    public static AllowList read(final String file) throws Exception {
        final InputStream stream = new FileInputStream(new File(file));

        final Constructor constructor = new Constructor(AllowList.class);
        final TypeDescription allowListDescription = new TypeDescription(AllowList.class);
        allowListDescription.putMapPropertyType("grants", String.class, Object.class);
        allowListDescription.putMapPropertyType("groups", String.class, Object.class);
        constructor.addTypeDescription(allowListDescription);

        final Yaml yaml = new Yaml(constructor);
        return yaml.loadAs(stream, AllowList.class);
    }
}
