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

package org.apache.tinkerpop.gremlin.dotnet

import org.apache.tinkerpop.gremlin.util.CoreImports
import org.apache.tinkerpop.gremlin.structure.Direction

class EnumGenerator {

    public static void create(final String enumDirectory) {

        Map<String, String> enumCSharpToJavaNames = new HashMap<String, String>();
        for (final Class<? extends Enum> enumClass : CoreImports.getClassImports()
                .findAll { Enum.class.isAssignableFrom(it) }
                .sort { a, b -> a.getSimpleName() <=> b.getSimpleName() }
                .collect()) {
            createEnum(enumDirectory, enumClass, enumCSharpToJavaNames)
        }

        // Write a file containing the equivalence in names between Java and C#
        final String enumCSharpToJavaFile = "$enumDirectory/NamingConversions.cs"
        final File file = new File(enumCSharpToJavaFile);
        file.delete();
        file.append(CommonContentHelper.getLicense());
        file.append("""
using System.Collections.Generic;

namespace Gremlin.Net.Process.Traversal
{
    internal static class NamingConversions
    {
        /// <summary>
        /// Gets the Java name equivalent for a given enum value
        /// </summary>
        internal static string GetEnumJavaName(string typeName, string value)
        {
            var key = \$"{typeName}.{value}";
            string javaName;
            if (!CSharpToJavaEnums.TryGetValue(key, out javaName))
            {
                throw new KeyNotFoundException(\$"Java name for {key} not found");
            }
            return javaName;
        }

        internal static readonly IDictionary<string, string> CSharpToJavaEnums = new Dictionary<string, string>
        {
"""     );
        def lastIndex = (enumCSharpToJavaNames.size() - 1);
        enumCSharpToJavaNames.eachWithIndex{ node, i ->
            file.append("""            {"$node.key", "$node.value"}${i == lastIndex ? "" : ","}\n""")
        }
        file.append("        };\n    }\n}");

    }

    public static String toCSharpName(final Class<? extends Enum> enumClass, String itemName) {
        if (enumClass.equals(Direction.class)) {
            itemName = itemName.toLowerCase();
        }
        return itemName.substring(0, 1).toUpperCase() + itemName.substring(1);
    }

    private static void createEnum(final String enumDirectory, final Class<? extends Enum> enumClass,
                                   final Map<String, String> csharpToJava) {
        final StringBuilder csharpEnum = new StringBuilder()

        csharpEnum.append(CommonContentHelper.getLicense())

        csharpEnum.append(
                """
namespace Gremlin.Net.Process.Traversal
{
    public enum ${enumClass.getSimpleName()}
    {
""")
        enumClass.getEnumConstants().
                sort { a, b -> a.name() <=> b.name() }.
                each { value ->
                    def csharpName = toCSharpName(enumClass, value.name());
                    csharpEnum.append("        $csharpName,\n");
                    csharpToJava.put(enumClass.simpleName + "." + csharpName, value.name());
                }
        csharpEnum.deleteCharAt(csharpEnum.length() - 2)

        csharpEnum.append("    }\n")
        csharpEnum.append("}")

        final String enumFileName = "${enumDirectory}/${enumClass.getSimpleName()}.cs"
        final File file = new File(enumFileName);
        file.delete()
        csharpEnum.eachLine { file.append(it + "\n") }
    }
}

