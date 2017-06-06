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

package org.apache.tinkerpop.gremlin.csharp

import org.apache.tinkerpop.gremlin.util.CoreImports

class EnumGenerator {

    public static void create(final String enumDirectory) {

        for (final Class<? extends Enum> enumClass : CoreImports.getClassImports()
                .findAll { Enum.class.isAssignableFrom(it) }
                .sort { a, b -> a.getSimpleName() <=> b.getSimpleName() }
                .collect()) {
            createEnum(enumDirectory, enumClass)
        }
    }

    private static void createEnum(final String enumDirectory, final Class<? extends Enum> enumClass){
        final StringBuilder csharpEnum = new StringBuilder()

        csharpEnum.append(CommonContentHelper.getLicense())

        csharpEnum.append(
                """
namespace Gremlin.Net.Process.Traversal
{
    public enum ${enumClass.getSimpleName()}
    {
""")
        enumClass.getEnumConstants()
                .sort { a, b -> a.name() <=> b.name() }
                .each { value -> csharpEnum.append("        ${value.name()},\n"); }
        csharpEnum.deleteCharAt(csharpEnum.length() - 2)

        csharpEnum.append("    }\n")
        csharpEnum.append("}")

        final String enumFileName = "${enumDirectory}/${enumClass.getSimpleName()}.cs"
        final File file = new File(enumFileName);
        file.delete()
        csharpEnum.eachLine { file.append(it + "\n") }
    }
}

