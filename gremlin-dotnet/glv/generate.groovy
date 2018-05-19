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

import org.apache.tinkerpop.gremlin.jsr223.CoreImports
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.P
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__
import org.apache.tinkerpop.gremlin.structure.Direction
import java.lang.reflect.Modifier
import java.lang.reflect.TypeVariable
import java.lang.reflect.GenericArrayType

def toCSharpTypeMap = ["Long": "long",
                       "Integer": "int",
                       "String": "string",
                       "boolean": "bool",
                       "Object": "object",
                       "String[]": "string[]",
                       "Object[]": "object[]",
                       "Class": "Type",
                       "Class[]": "Type[]",
                       "java.util.Map<java.lang.String, E2>": "IDictionary<string, E2>",
                       "java.util.Map<java.lang.String, B>": "IDictionary<string, E2>",
                       "java.util.List<E>": "IList<E>",
                       "java.util.List<A>": "IList<E2>",
                       "java.util.Map<K, V>": "IDictionary<K, V>",
                       "java.util.Collection<E2>": "ICollection<E2>",
                       "java.util.Collection<B>": "ICollection<E2>",
                       "java.util.Map<K, java.lang.Long>": "IDictionary<K, long>",
                       "TraversalMetrics": "E2",
                       "Traversal": "ITraversal",
                       "Traversal[]": "ITraversal[]",
                       "Predicate": "IPredicate",
                       "P": "P",
                       "TraversalStrategy": "ITraversalStrategy",
                       "TraversalStrategy[]": "ITraversalStrategy[]",
                       "Function": "IFunction",
                       "BiFunction": "IBiFunction",
                       "UnaryOperator": "IUnaryOperator",
                       "BinaryOperator": "IBinaryOperator",
                       "Consumer": "IConsumer",
                       "Supplier": "ISupplier",
                       "Comparator": "IComparator",
                       "VertexProgram": "object"]

def useE2 = ["E2", "E2"];
def methodsWithSpecificTypes = ["constant": useE2,
                                "limit": useE2,
                                "mean": useE2,
                                "optional": useE2,
                                "range": useE2,
                                "sum": useE2,
                                "tail": useE2,
                                "unfold": useE2]                                                        

def getCSharpGenericTypeParam = { typeName ->
    def tParam = ""
    if (typeName.contains("E2")) {
        tParam = "<E2>"
    }
    else if (typeName.contains("<K, V>")) {
        tParam = "<K, V>"
    }
    else if (typeName.contains("<K, ")) {
        tParam = "<K>"
    }
    else if (typeName.contains("S")) {
        tParam = "<S>"
    }
    else if (typeName.contains("A")) {
        tParam = "<A>"
    }
    return tParam
}

def toCSharpType = { name ->
    String typeName = toCSharpTypeMap.getOrDefault(name, name);
    if (typeName.equals(name) && (typeName.contains("? extends") || typeName.equals("Tree"))) {
        typeName = "E2"
    }
    return typeName;
}

def toCSharpMethodName = { symbol -> (String) Character.toUpperCase(symbol.charAt(0)) + symbol.substring(1) }

def getJavaGenericTypeParameterTypeNames = { method ->
    def typeArguments = method.genericReturnType.actualTypeArguments;
    return typeArguments.
            collect { (it instanceof Class) ? ((Class)it).simpleName : it.typeName }.
            collect { name ->
                if (name.equals("A")) {
                    name = "object"
                }
                else if (name.equals("B")) {
                    name = "E2";
                }
                name
            }
}

def getJavaParameterTypeNames = { method ->
    return method.parameters.
            collect { param ->
                param.type.simpleName
            } 
}

def toCSharpParamString = { param, genTypeName ->
    String csharpParamTypeName = genTypeName;
    if (csharpParamTypeName == null){
        csharpParamTypeName = toCSharpType(param.type.simpleName)
    }
    else if (csharpParamTypeName == "M") {
        csharpParamTypeName = "object";
    }
    else if (csharpParamTypeName == "A[]") {
        csharpParamTypeName = "object[]";
    }
    else if (csharpParamTypeName == "A" || csharpParamTypeName == "B") {
        csharpParamTypeName = "E2";
    }
    "${csharpParamTypeName} ${param.name}"
    }

def getJavaParamTypeString = { method ->
    getJavaParameterTypeNames(method).join(",")
}

def getCSharpParamTypeString = { method ->
    return method.parameters.
            collect { param ->
                toCSharpType(param.type.simpleName)
            }.join(",")
}

def getCSharpParamString = { method, useGenericParams ->
    def parameters = method.parameters;
    if (parameters.length == 0)
        return ""

    def genericTypes = method.getGenericParameterTypes();
    def csharpParameters = parameters.
            toList().indexed().
            collect { index, param ->
                String genTypeName = null
                if (useGenericParams) {
                    def genType = genericTypes[index]
                    if (genType instanceof TypeVariable<?>) {
                        genTypeName = ((TypeVariable<?>)genType).name
                    }
                    else if (genType instanceof GenericArrayType) {
                        if (((GenericArrayType)genType).getGenericComponentType() instanceof TypeVariable<?>) {
                            genTypeName = ((TypeVariable<?>)((GenericArrayType)genType).getGenericComponentType()).name + "[]"
                        }                        
                    }
                }
                toCSharpParamString(param, genTypeName)
            }.
            toArray();

    if (method.isVarArgs()) {
        def lastIndex = csharpParameters.length-1;
        csharpParameters[lastIndex] = "params " + csharpParameters[lastIndex];
    }

    csharpParameters.join(", ")
}

def getParamNames = { parameters ->
    return parameters.
        collect { param ->
            param.name
        }
}

def getArgsListType = { parameterString ->
    def argsListType = "object"
    if (parameterString.contains("params ")) {
        def paramsType = parameterString.substring(parameterString.indexOf("params ") + "params ".length(), parameterString.indexOf("[]"))
        if (paramsType == "E" || paramsType == "S")
            argsListType = paramsType
    }
    argsListType
}

def hasMethodNoGenericCounterPartInGraphTraversal = { method ->
    def parameterTypeNames = getJavaParameterTypeNames(method)
    if (method.name.equals("fold")) {
        return parameterTypeNames.size() == 0
    }
    if (method.name.equals("limit")) {
        if (parameterTypeNames.size() == 1) {
            return parameterTypeNames[0].equals("long")
        }
    }
    if (method.name.equals("range")) {
        if (parameterTypeNames.size() == 2) {
            return parameterTypeNames[0].equals("long") && parameterTypeNames[1].equals("long")
        }
    }
    if (method.name.equals("tail")) {
        if (parameterTypeNames.size() == 0) {
            return true
        }
        if (parameterTypeNames.size() == 1) {
            return parameterTypeNames[0].equals("long")
        }
    }
    return false
}

def t2withSpecialGraphTraversalt2 = ["IList<E2>": "E2"]

def getGraphTraversalT2ForT2 = { t2 ->
    if (t2withSpecialGraphTraversalt2.containsKey(t2)) {
        return t2withSpecialGraphTraversalt2.get(t2)
    }
    return t2
}

def binding = ["pmethods": P.class.getMethods().
        findAll { Modifier.isStatic(it.getModifiers()) }.
        findAll { P.class.isAssignableFrom(it.returnType) }.
        collect { it.name }.
        unique().
        sort { a, b -> a <=> b },
               "sourceStepMethods": GraphTraversalSource.getMethods(). // SOURCE STEPS
                        findAll { GraphTraversalSource.class.equals(it.returnType) }.
                        findAll {
                            !it.name.equals("clone") &&
                                    !it.name.equals(TraversalSource.Symbols.withBindings) &&
                                    !it.name.equals(TraversalSource.Symbols.withRemote) &&
                                    !it.name.equals(TraversalSource.Symbols.withComputer)
                        }.
                // Select unique combination of C# parameter types and sort by Java parameter type combination
                        sort { a, b -> a.name <=> b.name ?: getJavaParamTypeString(a) <=> getJavaParamTypeString(b) }.
                        unique { a,b -> a.name <=> b.name ?: getCSharpParamTypeString(a) <=> getCSharpParamTypeString(b) }.
                        collect { javaMethod ->
                            def parameters = getCSharpParamString(javaMethod, false)
                            def paramNames = getParamNames(javaMethod.parameters)
                            return ["methodName": javaMethod.name, "parameters":parameters, "paramNames":paramNames]
                        },
               "sourceSpawnMethods": GraphTraversalSource.getMethods(). // SPAWN STEPS
                        findAll { GraphTraversal.class.equals(it.returnType) }.          
                // Select unique combination of C# parameter types and sort by Java parameter type combination                                                                    
                        sort { a, b -> a.name <=> b.name ?: getJavaParamTypeString(a) <=> getJavaParamTypeString(b) }.
                        unique { a,b -> a.name <=> b.name ?: getCSharpParamTypeString(a) <=> getCSharpParamTypeString(b) }.
                        collect { javaMethod ->
                            def typeNames = getJavaGenericTypeParameterTypeNames(javaMethod)
                            def typeNameString = typeNames.join(", ")
                            def t2 = toCSharpType(typeNames[1])
                            def tParam = getCSharpGenericTypeParam(t2)
                            def parameters = getCSharpParamString(javaMethod, true)
                            def paramNames = getParamNames(javaMethod.parameters)
                            def argsListType = getArgsListType(parameters)
                            return ["methodName": javaMethod.name, "typeNameString": typeNameString, "tParam":tParam, "parameters":parameters, "paramNames":paramNames, "argsListType":argsListType]
                        },
               "graphStepMethods": GraphTraversal.getMethods().
                        findAll { GraphTraversal.class.equals(it.returnType) }.
                        findAll { !it.name.equals("clone") && !it.name.equals("iterate") }.
                // Select unique combination of C# parameter types and sort by Java parameter type combination
                        sort { a, b -> a.name <=> b.name ?: getJavaParamTypeString(a) <=> getJavaParamTypeString(b) }.
                        unique { a,b -> a.name <=> b.name ?: getCSharpParamTypeString(a) <=> getCSharpParamTypeString(b) }.
                        collect { javaMethod ->
                            def typeNames = getJavaGenericTypeParameterTypeNames(javaMethod)
                            def t1 = toCSharpType(typeNames[0])
                            def t2 = toCSharpType(typeNames[1])
                            def tParam = getCSharpGenericTypeParam(t2)
                            def parameters = getCSharpParamString(javaMethod, true)
                            def paramNames = getParamNames(javaMethod.parameters)
                            def argsListType = getArgsListType(parameters)
                            return ["methodName": javaMethod.name, "t1":t1, "t2":t2, "tParam":tParam, "parameters":parameters, "paramNames":paramNames, "argsListType":argsListType]
                        },
               "anonStepMethods": __.class.getMethods().
                        findAll { GraphTraversal.class.equals(it.returnType) }.
                        findAll { Modifier.isStatic(it.getModifiers()) }.
                        findAll { !it.name.equals("__") && !it.name.equals("start") }.
                // Select unique combination of C# parameter types and sort by Java parameter type combination
                        sort { a, b -> a.name <=> b.name ?: getJavaParamTypeString(a) <=> getJavaParamTypeString(b) }.
                        unique { a,b -> a.name <=> b.name ?: getCSharpParamTypeString(a) <=> getCSharpParamTypeString(b) }.
                        collect { javaMethod ->
                            def typeNames = getJavaGenericTypeParameterTypeNames(javaMethod)
                            def t2 = toCSharpType(typeNames[1])
                            def tParam = getCSharpGenericTypeParam(t2)
                            def specificTypes = methodsWithSpecificTypes.get(javaMethod.name)
                            if (specificTypes) {
                                t2 = specificTypes[0]
                                tParam = specificTypes.size() > 1 ? "<" + specificTypes[1] + ">" : ""
                            }
                            def parameters = getCSharpParamString(javaMethod, true)
                            def paramNames = getParamNames(javaMethod.parameters)
                            def callGenericTypeArg = hasMethodNoGenericCounterPartInGraphTraversal(javaMethod) ? "" : tParam
                            def graphTraversalT2 = getGraphTraversalT2ForT2(t2)
                            return ["methodName": javaMethod.name, "t2":t2, "tParam":tParam, "parameters":parameters, "paramNames":paramNames, "callGenericTypeArg":callGenericTypeArg, "graphTraversalT2":graphTraversalT2]
                        },
               "toCSharpMethodName": toCSharpMethodName]

def engine = new groovy.text.GStringTemplateEngine()
def traversalTemplate = engine.createTemplate(new File("${projectBaseDir}/glv/GraphTraversal.template")).make(binding)
def traversalFile = new File("${projectBaseDir}/src/Gremlin.Net/Process/Traversal/GraphTraversal.cs")
traversalFile.newWriter().withWriter{ it << traversalTemplate }

def graphTraversalTemplate = engine.createTemplate(new File("${projectBaseDir}/glv/GraphTraversalSource.template")).make(binding)
def graphTraversalFile = new File("${projectBaseDir}/src/Gremlin.Net/Process/Traversal/GraphTraversalSource.cs")
graphTraversalFile.newWriter().withWriter{ it << graphTraversalTemplate }

def anonymousTraversalTemplate = engine.createTemplate(new File("${projectBaseDir}/glv/AnonymousTraversal.template")).make(binding)
def anonymousTraversalFile = new File("${projectBaseDir}/src/Gremlin.Net/Process/Traversal/__.cs")
anonymousTraversalFile.newWriter().withWriter{ it << anonymousTraversalTemplate }

def pTemplate = engine.createTemplate(new File("${projectBaseDir}/glv/P.template")).make(binding)
def pFile = new File("${projectBaseDir}/src/Gremlin.Net/Process/Traversal/P.cs")
pFile.newWriter().withWriter{ it << pTemplate }

// Process enums
def toCSharpName = { enumClass, itemName ->
    if (enumClass.equals(Direction.class)) {
        itemName = itemName.toLowerCase()
    }

    return itemName.substring(0, 1).toUpperCase() + itemName.substring(1)
}

def createEnum = { enumClass ->
    def b = ["enumClass": enumClass,
             "implementedTypes": enumClass.getInterfaces().
                    collect { it.getSimpleName() }.
                    findAll { toCSharpTypeMap.containsKey(it) }.
                    findAll { toCSharpTypeMap[it] != "object" }.
                    sort { a, b -> a <=> b }.
                    collect(["EnumWrapper"]) { typeName ->
                        return toCSharpTypeMap[typeName]
                    }.join(", "),
             "constants": enumClass.getEnumConstants().
                    sort { a, b -> a.name() <=> b.name() }.
                    collect { value ->
                        def csharpName = toCSharpName(enumClass, value.name())
                        return "public static ${enumClass.simpleName} ${csharpName} => new ${enumClass.simpleName}(\"${value.name()}\");"
                    }]

    def enumTemplate = engine.createTemplate(new File("${projectBaseDir}/glv/Enum.template")).make(b)
    def enumFile = new File("${projectBaseDir}/src/Gremlin.Net/Process/Traversal/" + enumClass.getSimpleName() + ".cs")
    enumFile.newWriter().withWriter{ it << enumTemplate }
}

CoreImports.getClassImports().findAll { Enum.class.isAssignableFrom(it) }.
        sort { a, b -> a.getSimpleName() <=> b.getSimpleName() }.
        each { createEnum(it) }

def determineVersion = {
    def env = System.getenv()
    def mavenVersion = env.containsKey("TP_RELEASE_VERSION") ? env.get("DOTNET_RELEASE_VERSION") : "${projectVersion}"

    // only want to generate a timestamp for the version if this is a nuget deploy
    if (!mavenVersion.endsWith("-SNAPSHOT") || null == System.getProperty("nuget")) return mavenVersion

    return mavenVersion.replace("-SNAPSHOT", "-dev-" + System.currentTimeMillis())
}

def versionToUse = determineVersion()
def csprojTemplate = engine.createTemplate(new File("${projectBaseDir}/glv/Gremlin.Net.csproj.template")).make(["projectVersion":versionToUse])
def csprojFile = new File("${projectBaseDir}/src/Gremlin.Net/Gremlin.Net.csproj")
csprojFile.newWriter().withWriter{ it << csprojTemplate }
def templateCsprojTemplate = engine.createTemplate(new File("${projectBaseDir}/glv/Gremlin.Net.Template.csproj.template")).make(["projectVersion":versionToUse])
def templateCsprojFile = new File("${projectBaseDir}/src/Gremlin.Net.Template/Gremlin.Net.Template.csproj")
templateCsprojFile.newWriter().withWriter{ it << templateCsprojTemplate }
def nuspecTemplate = engine.createTemplate(new File("${projectBaseDir}/glv/Gremlin.Net.Template.nuspec.template")).make(["projectVersion":versionToUse])
def nuspecFile = new File("${projectBaseDir}/src/Gremlin.Net.Template/Gremlin.Net.Template.nuspec")
nuspecFile.newWriter().withWriter{ it << nuspecTemplate }