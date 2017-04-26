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
package org.apache.tinkerpop.gremlin.process.traversal.dsl;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.TypeVariableName;
import jdk.nashorn.internal.codegen.types.Type;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.TypeParameterElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@SupportedAnnotationTypes("org.apache.tinkerpop.gremlin.process.traversal.dsl.GremlinDsl")
@SupportedSourceVersion(SourceVersion.RELEASE_8)
public class GremlinDslProcessor extends AbstractProcessor {
    private Messager messager;
    private Elements elementUtils;
    private Filer filer;
    private Types typeUtils;

    @Override
    public synchronized void init(final ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
        messager = processingEnv.getMessager();
        elementUtils = processingEnv.getElementUtils();
        filer = processingEnv.getFiler();
        typeUtils = processingEnv.getTypeUtils();
    }

    @Override
    public boolean process(final Set<? extends TypeElement> annotations, final RoundEnvironment roundEnv) {
        try {
            for (Element dslElement : roundEnv.getElementsAnnotatedWith(GremlinDsl.class)) {
                validateDSL(dslElement);

                final TypeElement annotatedDslType = (TypeElement) dslElement;
                final String packageName = elementUtils.getPackageOf(dslElement).getQualifiedName().toString();

                // create the Traversal implementation interface
                final String dslName = dslElement.getSimpleName().toString();
                final String dslPrefix = dslName.substring(0, dslName.length() - "TraversalDSL".length()); // chop off "TraversalDSL"
                final String traversalClazz = dslPrefix + "Traversal";
                final ClassName traversalClassName = ClassName.get(packageName, traversalClazz);
                final String traversalSourceClazz = dslPrefix + "TraversalSource";
                final ClassName traversalSourceClassName = ClassName.get(packageName, traversalSourceClazz);
                final String defaultTraversalClazz = "Default" + traversalClazz;
                final ClassName defaultTraversalClassName = ClassName.get(packageName, defaultTraversalClazz);
                final ClassName graphTraversalAdminClassName = ClassName.get(GraphTraversal.Admin.class);

                // START write "Traversal" class
                final TypeSpec.Builder traversalInterface = TypeSpec.interfaceBuilder(traversalClazz)
                        .addModifiers(Modifier.PUBLIC)
                        .addTypeVariables(Arrays.asList(TypeVariableName.get("S"), TypeVariableName.get("E")))
                        .addSuperinterface(TypeName.get(dslElement.asType()));

                // process the methods of the GremlinDsl annotated class
                for (Element elementOfDsl : annotatedDslType.getEnclosedElements()) {
                    tryConstructMethod(elementOfDsl, traversalClassName, dslName, null,
                            Modifier.PUBLIC, Modifier.DEFAULT).ifPresent(traversalInterface::addMethod);
                }

                // process the methods of GraphTraversal
                final TypeElement graphTraversalElement = elementUtils.getTypeElement(GraphTraversal.class.getCanonicalName());
                for (Element elementOfGraphTraversal : graphTraversalElement.getEnclosedElements()) {
                    tryConstructMethod(elementOfGraphTraversal, traversalClassName, dslName,
                            e -> e.getSimpleName().contentEquals("asAdmin") || e.getSimpleName().contentEquals("iterate"),
                            Modifier.PUBLIC, Modifier.DEFAULT)
                            .ifPresent(traversalInterface::addMethod);
                }

                final JavaFile traversalJavaFile = JavaFile.builder(packageName, traversalInterface.build()).build();
                traversalJavaFile.writeTo(filer);
                // END write "Traversal" class

                // START write of the "DefaultTraversal" class
                final TypeSpec.Builder defaultTraversalClass = TypeSpec.classBuilder(defaultTraversalClazz)
                        .addModifiers(Modifier.PUBLIC)
                        .addTypeVariables(Arrays.asList(TypeVariableName.get("S"), TypeVariableName.get("E")))
                        .superclass(TypeName.get(elementUtils.getTypeElement(DefaultTraversal.class.getCanonicalName()).asType()))
                        .addSuperinterface(ParameterizedTypeName.get(traversalClassName, TypeVariableName.get("S"), TypeVariableName.get("E")));

                // add the required constructors for instantiation
                defaultTraversalClass.addMethod(MethodSpec.constructorBuilder()
                        .addModifiers(Modifier.PUBLIC)
                        .addStatement("super()")
                        .build());
                defaultTraversalClass.addMethod(MethodSpec.constructorBuilder()
                        .addModifiers(Modifier.PUBLIC)
                        .addParameter(Graph.class, "graph")
                        .addStatement("super($N)", "graph")
                        .build());
                defaultTraversalClass.addMethod(MethodSpec.constructorBuilder()
                        .addModifiers(Modifier.PUBLIC)
                        .addParameter(traversalSourceClassName, "traversalSource")
                        .addStatement("super($N)", "traversalSource")
                        .build());

                // add the override
                defaultTraversalClass.addMethod(MethodSpec.methodBuilder("iterate")
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class)
                        .addStatement("return ($T) super.iterate()", traversalClassName)
                        .returns(ParameterizedTypeName.get(traversalClassName, TypeVariableName.get("S"), TypeVariableName.get("E")))
                        .build());
                defaultTraversalClass.addMethod(MethodSpec.methodBuilder("asAdmin")
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class)
                        .addStatement("return ($T) super.asAdmin()", GraphTraversal.Admin.class)
                        .returns(ParameterizedTypeName.get(graphTraversalAdminClassName, TypeVariableName.get("S"), TypeVariableName.get("E")))
                        .build());
                defaultTraversalClass.addMethod(MethodSpec.methodBuilder("clone")
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class)
                        .addStatement("return ($T) super.clone()", defaultTraversalClassName)
                        .returns(ParameterizedTypeName.get(defaultTraversalClassName, TypeVariableName.get("S"), TypeVariableName.get("E")))
                        .build());

                final JavaFile defaultTraversalJavaFile = JavaFile.builder(packageName, defaultTraversalClass.build()).build();
                defaultTraversalJavaFile.writeTo(filer);
                // END write of the "DefaultTraversal" class

                // START write "TraversalSource" class
                final TypeElement graphTraversalSourceElement = elementUtils.getTypeElement(GraphTraversalSource.class.getCanonicalName());
                final TypeSpec.Builder traversalSourceClass = TypeSpec.classBuilder(traversalSourceClazz)
                        .addModifiers(Modifier.PUBLIC)
                        .superclass(TypeName.get(graphTraversalSourceElement.asType()));

                // add the required constructors for instantiation
                traversalSourceClass.addMethod(MethodSpec.constructorBuilder()
                        .addModifiers(Modifier.PUBLIC)
                        .addParameter(Graph.class, "graph")
                        .addStatement("super($N)", "graph")
                        .build());
                traversalSourceClass.addMethod(MethodSpec.constructorBuilder()
                        .addModifiers(Modifier.PUBLIC)
                        .addParameter(Graph.class, "graph")
                        .addParameter(TraversalStrategies.class, "strategies")
                        .addStatement("super($N, $N)", "graph", "strategies")
                        .build());

                // override methods to return a the DSL TraversalSource
                for (Element elementOfGraphTraversal : graphTraversalSourceElement.getEnclosedElements()) {
                    // first copy/override methods that return a GraphTraversalSource so that we can instead return
                    // the DSL TraversalSource class.
                    tryConstructMethod(elementOfGraphTraversal, traversalSourceClassName, "",
                            e -> !(e.getReturnType().getKind() == TypeKind.DECLARED && ((DeclaredType) e.getReturnType()).asElement().getSimpleName().contentEquals(GraphTraversalSource.class.getSimpleName())),
                            Modifier.PUBLIC)
                            .ifPresent(traversalSourceClass::addMethod);
                }

                // override methods that return GraphTraversal
                traversalSourceClass.addMethod(MethodSpec.methodBuilder("addV")
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class)
                        .addStatement("$N clone = this.clone()", traversalSourceClazz)
                        .addStatement("clone.bytecode.addStep($T.addV)", GraphTraversal.Symbols.class)
                        .addStatement("$N traversal = new $N(clone)", defaultTraversalClazz, defaultTraversalClazz)
                        .addStatement("return ($T) traversal.asAdmin().addStep(new $T(traversal, null))", traversalClassName, AddVertexStartStep.class)
                        .returns(ParameterizedTypeName.get(traversalClassName, ClassName.get(Vertex.class), ClassName.get(Vertex.class)))
                        .build());
                traversalSourceClass.addMethod(MethodSpec.methodBuilder("addV")
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class)
                        .addParameter(String.class, "label")
                        .addStatement("$N clone = this.clone()", traversalSourceClazz)
                        .addStatement("clone.bytecode.addStep($T.addV, label)", GraphTraversal.Symbols.class)
                        .addStatement("$N traversal = new $N(clone)", defaultTraversalClazz, defaultTraversalClazz)
                        .addStatement("return ($T) traversal.asAdmin().addStep(new $T(traversal, label))", traversalClassName, AddVertexStartStep.class)
                        .returns(ParameterizedTypeName.get(traversalClassName, ClassName.get(Vertex.class), ClassName.get(Vertex.class)))
                        .build());
                traversalSourceClass.addMethod(MethodSpec.methodBuilder("V")
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class)
                        .addParameter(Object[].class, "vertexIds")
                        .varargs(true)
                        .addStatement("$N clone = this.clone()", traversalSourceClazz)
                        .addStatement("clone.bytecode.addStep($T.V, vertexIds)", GraphTraversal.Symbols.class)
                        .addStatement("$N traversal = new $N(clone)", defaultTraversalClazz, defaultTraversalClazz)
                        .addStatement("return ($T) traversal.asAdmin().addStep(new $T(traversal, $T.class, true, vertexIds))", traversalClassName, GraphStep.class, Vertex.class)
                        .returns(ParameterizedTypeName.get(traversalClassName, ClassName.get(Vertex.class), ClassName.get(Vertex.class)))
                        .build());
                traversalSourceClass.addMethod(MethodSpec.methodBuilder("E")
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class)
                        .addParameter(Object[].class, "edgeIds")
                        .varargs(true)
                        .addStatement("$N clone = this.clone()", traversalSourceClazz)
                        .addStatement("clone.bytecode.addStep($T.E, edgeIds)", GraphTraversal.Symbols.class)
                        .addStatement("$N traversal = new $N(clone)", defaultTraversalClazz, defaultTraversalClazz)
                        .addStatement("return ($T) traversal.asAdmin().addStep(new $T(traversal, $T.class, true, edgeIds))", traversalClassName, GraphStep.class, Edge.class)
                        .returns(ParameterizedTypeName.get(traversalClassName, ClassName.get(Edge.class), ClassName.get(Edge.class)))
                        .build());

                final JavaFile traversalSourceJavaFile = JavaFile.builder(packageName, traversalSourceClass.build()).build();
                traversalSourceJavaFile.writeTo(filer);
                // END write "TraversalSource" class
            }
        } catch (Exception ex) {
            messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
        }

        return true;
    }

    private Optional<MethodSpec> tryConstructMethod(final Element element, final ClassName returnClazz, final String parent,
                                                    final Predicate<ExecutableElement> ignore, final Modifier... modifiers) {
        if (element.getKind() != ElementKind.METHOD) return Optional.empty();

        final ExecutableElement templateMethod = (ExecutableElement) element;
        final String methodName = templateMethod.getSimpleName().toString();

        if (ignore != null && ignore.test(templateMethod)) return Optional.empty();

        final DeclaredType returnTypeMirror = (DeclaredType) templateMethod.getReturnType();
        final List<? extends TypeMirror> returnTypeArguments = returnTypeMirror.getTypeArguments();

        // build a return type with appropriate generic declarations (if such declarations are present)
        final TypeName returnType = returnTypeArguments.isEmpty() ?
                returnClazz :
                ParameterizedTypeName.get(returnClazz, returnTypeArguments.stream().map(TypeName::get).collect(Collectors.toList()).toArray(new TypeName[returnTypeArguments.size()]));
        final MethodSpec.Builder methodToAdd = MethodSpec.methodBuilder(methodName)
                .addModifiers(modifiers)
                .addAnnotation(Override.class)
                .addExceptions(templateMethod.getThrownTypes().stream().map(TypeName::get).collect(Collectors.toList()))
                .returns(returnType);

        templateMethod.getTypeParameters().forEach(tp -> methodToAdd.addTypeVariable(TypeVariableName.get(tp)));

        boolean added = false;
        final List<? extends VariableElement> parameters = templateMethod.getParameters();
        final String parentCall = parent.isEmpty() ? "" : parent + ".";
        String body = "return (" + returnClazz.simpleName() + ") " + parentCall + "super." + methodName + "(";
        for (VariableElement param : parameters) {
            methodToAdd.addParameter(ParameterSpec.get(param));

            body = body + param.getSimpleName() + ",";
            added = true;
        }

        // treat a final array as a varargs param
        if (!parameters.isEmpty() && parameters.get(parameters.size() - 1).asType().getKind() == TypeKind.ARRAY)
            methodToAdd.varargs(true);

        if (added) body = body.substring(0, body.length() - 1);

        body = body + ")";
        methodToAdd.addStatement(body);

        return Optional.of(methodToAdd.build());
    }

    private void validateDSL(final Element dslElement) throws ProcessorException {
        if (dslElement.getKind() != ElementKind.INTERFACE)
            throw new ProcessorException(dslElement, "Only interfaces can be annotated with @%s", GremlinDsl.class.getSimpleName());

        final TypeElement typeElement = (TypeElement) dslElement;
        if (!typeElement.getModifiers().contains(Modifier.PUBLIC))
            throw new ProcessorException(dslElement, "The interface %s is not public.", typeElement.getQualifiedName());
    }
}
