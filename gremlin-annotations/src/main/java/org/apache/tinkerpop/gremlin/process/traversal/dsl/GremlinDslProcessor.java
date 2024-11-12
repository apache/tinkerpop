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

import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.TypeVariableName;
import com.squareup.javapoet.WildcardTypeName;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
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
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.type.TypeVariable;
import javax.lang.model.type.WildcardType;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A custom Java annotation processor for the {@link GremlinDsl} annotation that helps to generate DSLs classes.
 *
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
                final Context ctx = new Context((TypeElement) dslElement);

                // creates the "Traversal" interface using an extension of the GraphTraversal class that has the
                // GremlinDsl annotation on it
                generateTraversalInterface(ctx);

                // create the "DefaultTraversal" class which implements the above generated "Traversal" and can then
                // be used by the "TraversalSource" generated below to spawn new traversal instances.
                generateDefaultTraversal(ctx);

                // create the "TraversalSource" class which is used to spawn traversals from a Graph instance. It will
                // spawn instances of the "DefaultTraversal" generated above.
                generateTraversalSource(ctx);

                // create anonymous traversal for DSL
                generateAnonymousTraversal(ctx);
            }
        } catch (Exception ex) {
            messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
        }

        return true;
    }

    private void generateAnonymousTraversal(final Context ctx) throws IOException {
        final TypeSpec.Builder anonymousClass = TypeSpec.classBuilder("__")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL);

        // this class is just static methods - it should not be instantiated
        anonymousClass.addMethod(MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PRIVATE)
                .build());

        // add start() method
        anonymousClass.addMethod(MethodSpec.methodBuilder("start")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                .addTypeVariable(TypeVariableName.get("A"))
                .addStatement("return new $N<>()", ctx.defaultTraversalClazz)
                .returns(ParameterizedTypeName.get(ctx.traversalClassName, TypeVariableName.get("A"), TypeVariableName.get("A")))
                .build());

        // process the methods of the GremlinDsl annotated class
        for (ExecutableElement templateMethod : findMethodsOfElement(ctx.annotatedDslType, null)) {
            final Optional<GremlinDsl.AnonymousMethod> methodAnnotation = Optional.ofNullable(templateMethod.getAnnotation(GremlinDsl.AnonymousMethod.class));

            final String methodName = templateMethod.getSimpleName().toString();

            // either use the direct return type of the DSL specification or override it with specification from
            // GremlinDsl.AnonymousMethod
            final TypeName returnType = methodAnnotation.isPresent() && methodAnnotation.get().returnTypeParameters().length > 0 ?
                    getOverridenReturnTypeDefinition(ctx.traversalClassName, methodAnnotation.get().returnTypeParameters()) :
                    getReturnTypeDefinition(ctx.traversalClassName, templateMethod);
            
            final MethodSpec.Builder methodToAdd = MethodSpec.methodBuilder(methodName)
                    .addModifiers(Modifier.STATIC, Modifier.PUBLIC)
                    .addExceptions(templateMethod.getThrownTypes().stream().map(TypeName::get).collect(Collectors.toList()))
                    .returns(returnType);

            // either use the method type parameter specified from the GremlinDsl.AnonymousMethod or just infer them
            // from the DSL specification. "inferring" relies on convention and sometimes doesn't work for all cases.
            final String startGeneric = methodAnnotation.isPresent() && methodAnnotation.get().methodTypeParameters().length > 0 ?
                    methodAnnotation.get().methodTypeParameters()[0] : "S";
            if (methodAnnotation.isPresent() && methodAnnotation.get().methodTypeParameters().length > 0)
                Stream.of(methodAnnotation.get().methodTypeParameters()).map(TypeVariableName::get).forEach(methodToAdd::addTypeVariable);
            else {
                templateMethod.getTypeParameters().forEach(tp -> methodToAdd.addTypeVariable(TypeVariableName.get(tp)));

                // might have to deal with an "S" (in __ it's usually an "A") - how to make this less bound to that convention?
                final List<? extends TypeMirror> returnTypeArguments = getTypeArguments(templateMethod);
                returnTypeArguments.stream().filter(rtm -> rtm instanceof TypeVariable).forEach(rtm -> {
                    if (((TypeVariable) rtm).asElement().getSimpleName().contentEquals("S"))
                        methodToAdd.addTypeVariable(TypeVariableName.get(((TypeVariable) rtm).asElement().getSimpleName().toString()));
                });
            }

            addMethodBody(methodToAdd, templateMethod, "return __.<" + startGeneric + ">start().$L(", ")", methodName);
            anonymousClass.addMethod(methodToAdd.build());
        }

        // use methods from __ to template them into the DSL __
        final Element anonymousTraversal = elementUtils.getTypeElement(__.class.getCanonicalName());
        final Predicate<ExecutableElement> ignore = ee -> ee.getSimpleName().contentEquals("start");
        for (ExecutableElement templateMethod : findMethodsOfElement(anonymousTraversal, ignore)) {
            final String methodName = templateMethod.getSimpleName().toString();

            final TypeName returnType = getReturnTypeDefinition(ctx.traversalClassName, templateMethod);
            final MethodSpec.Builder methodToAdd = MethodSpec.methodBuilder(methodName)
                    .addModifiers(Modifier.STATIC, Modifier.PUBLIC)
                    .addExceptions(templateMethod.getThrownTypes().stream().map(TypeName::get).collect(Collectors.toList()))
                    .returns(returnType);

            templateMethod.getTypeParameters().forEach(tp -> methodToAdd.addTypeVariable(TypeVariableName.get(tp)));

            if (methodName.equals("__")) {
                for (VariableElement param : templateMethod.getParameters()) {
                    methodToAdd.addParameter(ParameterSpec.get(param));
                }

                methodToAdd.varargs(true);
                methodToAdd.addStatement("return inject(starts)", methodName);
            } else {
                if (templateMethod.getTypeParameters().isEmpty()) {
                    final List<? extends TypeMirror> types = getTypeArguments(templateMethod);
                    addMethodBody(methodToAdd, templateMethod, "return __.<$T>start().$L(", ")", types.get(0), methodName);
                } else {
                    addMethodBody(methodToAdd, templateMethod, "return __.<A>start().$L(", ")", methodName);
                }
            }

            anonymousClass.addMethod(methodToAdd.build());
        }

        final JavaFile traversalSourceJavaFile = JavaFile.builder(ctx.packageName, anonymousClass.build()).build();
        traversalSourceJavaFile.writeTo(filer);
    }

    private void generateTraversalSource(final Context ctx) throws IOException {
        final TypeElement graphTraversalSourceElement = ctx.traversalSourceDslType;
        final TypeSpec.Builder traversalSourceClass = TypeSpec.classBuilder(ctx.traversalSourceClazz)
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
        traversalSourceClass.addMethod(MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PUBLIC)
                .addParameter(RemoteConnection.class, "connection")
                .addStatement("super($N)", "connection")
                .build());

        // override methods to return the DSL TraversalSource. find GraphTraversalSource class somewhere in the hierarchy
        final Element tinkerPopsGraphTraversalSource = findClassAsElement(graphTraversalSourceElement, GraphTraversalSource.class);
        final Predicate<ExecutableElement> notGraphTraversalSourceReturnValues = e -> !(e.getReturnType().getKind() == TypeKind.DECLARED && ((DeclaredType) e.getReturnType()).asElement().getSimpleName().contentEquals(GraphTraversalSource.class.getSimpleName()));
        for (ExecutableElement elementOfGraphTraversalSource : findMethodsOfElement(tinkerPopsGraphTraversalSource, notGraphTraversalSourceReturnValues)) {
            // first copy/override methods that return a GraphTraversalSource so that we can instead return
            // the DSL TraversalSource class.
            traversalSourceClass.addMethod(constructMethod(elementOfGraphTraversalSource, ctx.traversalSourceClassName, "",Modifier.PUBLIC));
        }

        // override methods that return GraphTraversal that come from the user defined extension of GraphTraversal
        if (!graphTraversalSourceElement.getSimpleName().contentEquals(GraphTraversalSource.class.getSimpleName())) {
            final Predicate<ExecutableElement> notGraphTraversalReturnValues = e -> !(e.getReturnType().getKind() == TypeKind.DECLARED && ((DeclaredType) e.getReturnType()).asElement().getSimpleName().contentEquals(GraphTraversal.class.getSimpleName()));
            for (ExecutableElement templateMethod : findMethodsOfElement(graphTraversalSourceElement, notGraphTraversalReturnValues)) {
                final MethodSpec.Builder methodToAdd = MethodSpec.methodBuilder(templateMethod.getSimpleName().toString())
                        .addModifiers(Modifier.PUBLIC)
                        .addAnnotation(Override.class);

                methodToAdd.addStatement("$T clone = this.clone()", ctx.traversalSourceClassName);
                addMethodBody(methodToAdd, templateMethod, "return new $T (clone, super.$L(", ").asAdmin())",
                        ctx.defaultTraversalClassName, templateMethod.getSimpleName());
                methodToAdd.returns(getReturnTypeDefinition(ctx.traversalClassName, templateMethod));

                traversalSourceClass.addMethod(methodToAdd.build());
            }
        }

        if (ctx.generateDefaultMethods) {
            // override methods that return GraphTraversal
            traversalSourceClass.addMethod(MethodSpec.methodBuilder("addV")
                    .addModifiers(Modifier.PUBLIC)
                    .addAnnotation(Override.class)
                    .addStatement("$N clone = this.clone()", ctx.traversalSourceClazz)
                    .addStatement("clone.getGremlinLang().addStep($T.addV)", GraphTraversal.Symbols.class)
                    .addStatement("$N traversal = new $N(clone)", ctx.defaultTraversalClazz, ctx.defaultTraversalClazz)
                    .addStatement("return ($T) traversal.asAdmin().addStep(new $T(traversal, (String) null))", ctx.traversalClassName, AddVertexStartStep.class)
                    .returns(ParameterizedTypeName.get(ctx.traversalClassName, ClassName.get(Vertex.class), ClassName.get(Vertex.class)))
                    .build());
            traversalSourceClass.addMethod(MethodSpec.methodBuilder("addV")
                    .addModifiers(Modifier.PUBLIC)
                    .addAnnotation(Override.class)
                    .addParameter(String.class, "label")
                    .addStatement("$N clone = this.clone()", ctx.traversalSourceClazz)
                    .addStatement("clone.getGremlinLang().addStep($T.addV, label)", GraphTraversal.Symbols.class)
                    .addStatement("$N traversal = new $N(clone)", ctx.defaultTraversalClazz, ctx.defaultTraversalClazz)
                    .addStatement("return ($T) traversal.asAdmin().addStep(new $T(traversal, label))", ctx.traversalClassName, AddVertexStartStep.class)
                    .returns(ParameterizedTypeName.get(ctx.traversalClassName, ClassName.get(Vertex.class), ClassName.get(Vertex.class)))
                    .build());
            traversalSourceClass.addMethod(MethodSpec.methodBuilder("addV")
                    .addModifiers(Modifier.PUBLIC)
                    .addAnnotation(Override.class)
                    .addParameter(Traversal.class, "vertexLabelTraversal")
                    .addStatement("$N clone = this.clone()", ctx.traversalSourceClazz)
                    .addStatement("clone.getGremlinLang().addStep($T.addV, vertexLabelTraversal)", GraphTraversal.Symbols.class)
                    .addStatement("$N traversal = new $N(clone)", ctx.defaultTraversalClazz, ctx.defaultTraversalClazz)
                    .addStatement("return ($T) traversal.asAdmin().addStep(new $T(traversal, vertexLabelTraversal))", ctx.traversalClassName, AddVertexStartStep.class)
                    .returns(ParameterizedTypeName.get(ctx.traversalClassName, ClassName.get(Vertex.class), ClassName.get(Vertex.class)))
                    .build());
            traversalSourceClass.addMethod(MethodSpec.methodBuilder("addE")
                    .addModifiers(Modifier.PUBLIC)
                    .addAnnotation(Override.class)
                    .addParameter(String.class, "label")
                    .addStatement("$N clone = this.clone()", ctx.traversalSourceClazz)
                    .addStatement("clone.getGremlinLang().addStep($T.addE, label)", GraphTraversal.Symbols.class)
                    .addStatement("$N traversal = new $N(clone)", ctx.defaultTraversalClazz, ctx.defaultTraversalClazz)
                    .addStatement("return ($T) traversal.asAdmin().addStep(new $T(traversal, label))", ctx.traversalClassName, AddEdgeStartStep.class)
                    .returns(ParameterizedTypeName.get(ctx.traversalClassName, ClassName.get(Edge.class), ClassName.get(Edge.class)))
                    .build());
            traversalSourceClass.addMethod(MethodSpec.methodBuilder("addE")
                    .addModifiers(Modifier.PUBLIC)
                    .addAnnotation(Override.class)
                    .addParameter(Traversal.class, "edgeLabelTraversal")
                    .addStatement("$N clone = this.clone()", ctx.traversalSourceClazz)
                    .addStatement("clone.getGremlinLang().addStep($T.addE, edgeLabelTraversal)", GraphTraversal.Symbols.class)
                    .addStatement("$N traversal = new $N(clone)", ctx.defaultTraversalClazz, ctx.defaultTraversalClazz)
                    .addStatement("return ($T) traversal.asAdmin().addStep(new $T(traversal, edgeLabelTraversal))", ctx.traversalClassName, AddEdgeStartStep.class)
                    .returns(ParameterizedTypeName.get(ctx.traversalClassName, ClassName.get(Edge.class), ClassName.get(Edge.class)))
                    .build());
            traversalSourceClass.addMethod(MethodSpec.methodBuilder("V")
                    .addModifiers(Modifier.PUBLIC)
                    .addAnnotation(Override.class)
                    .addParameter(Object[].class, "vertexIds")
                    .varargs(true)
                    .addStatement("$N clone = this.clone()", ctx.traversalSourceClazz)
                    .addStatement("clone.getGremlinLang().addStep($T.V, vertexIds)", GraphTraversal.Symbols.class)
                    .addStatement("$N traversal = new $N(clone)", ctx.defaultTraversalClazz, ctx.defaultTraversalClazz)
                    .addStatement("return ($T) traversal.asAdmin().addStep(new $T(traversal, $T.class, true, vertexIds))", ctx.traversalClassName, GraphStep.class, Vertex.class)
                    .returns(ParameterizedTypeName.get(ctx.traversalClassName, ClassName.get(Vertex.class), ClassName.get(Vertex.class)))
                    .build());
            traversalSourceClass.addMethod(MethodSpec.methodBuilder("E")
                    .addModifiers(Modifier.PUBLIC)
                    .addAnnotation(Override.class)
                    .addParameter(Object[].class, "edgeIds")
                    .varargs(true)
                    .addStatement("$N clone = this.clone()", ctx.traversalSourceClazz)
                    .addStatement("clone.getGremlinLang().addStep($T.E, edgeIds)", GraphTraversal.Symbols.class)
                    .addStatement("$N traversal = new $N(clone)", ctx.defaultTraversalClazz, ctx.defaultTraversalClazz)
                    .addStatement("return ($T) traversal.asAdmin().addStep(new $T(traversal, $T.class, true, edgeIds))", ctx.traversalClassName, GraphStep.class, Edge.class)
                    .returns(ParameterizedTypeName.get(ctx.traversalClassName, ClassName.get(Edge.class), ClassName.get(Edge.class)))
                    .build());
            traversalSourceClass.addMethod(MethodSpec.methodBuilder("inject")
                    .addModifiers(Modifier.PUBLIC)
                    .addAnnotation(Override.class)
                    .addParameter(ArrayTypeName.of(TypeVariableName.get("S")), "starts")
                    .varargs(true)
                    .addTypeVariable(TypeVariableName.get("S"))
                    .addStatement("$N clone = this.clone()", ctx.traversalSourceClazz)
                    .addStatement("clone.getGremlinLang().addStep($T.inject, starts)", GraphTraversal.Symbols.class)
                    .addStatement("$N traversal = new $N(clone)", ctx.defaultTraversalClazz, ctx.defaultTraversalClazz)
                    .addStatement("return ($T) traversal.asAdmin().addStep(new $T(traversal, starts))", ctx.traversalClassName, InjectStep.class)
                    .returns(ParameterizedTypeName.get(ctx.traversalClassName, TypeVariableName.get("S"), TypeVariableName.get("S")))
                    .build());
            traversalSourceClass.addMethod(MethodSpec.methodBuilder("getAnonymousTraversalClass")
                    .addModifiers(Modifier.PUBLIC)
                    .addAnnotation(Override.class)
                    .addStatement("return Optional.of(__.class)")
                    .returns(ParameterizedTypeName.get(ClassName.get(Optional.class),
                             ParameterizedTypeName.get(ClassName.get(Class.class), WildcardTypeName.subtypeOf(Object.class))))
                    .build());
        }

        final JavaFile traversalSourceJavaFile = JavaFile.builder(ctx.packageName, traversalSourceClass.build()).build();
        traversalSourceJavaFile.writeTo(filer);
    }

    private Element findClassAsElement(final Element element, final Class<?> clazz) {
        if (element.getSimpleName().contentEquals(clazz.getSimpleName())) {
            return element;
        }

        final List<? extends TypeMirror> supertypes = typeUtils.directSupertypes(element.asType());
        return findClassAsElement(typeUtils.asElement(supertypes.get(0)), clazz);
    }

    private void generateDefaultTraversal(final Context ctx) throws IOException {
        final TypeSpec.Builder defaultTraversalClass = TypeSpec.classBuilder(ctx.defaultTraversalClazz)
                .addModifiers(Modifier.PUBLIC)
                .addTypeVariables(Arrays.asList(TypeVariableName.get("S"), TypeVariableName.get("E")))
                .superclass(TypeName.get(elementUtils.getTypeElement(DefaultTraversal.class.getCanonicalName()).asType()))
                .addSuperinterface(ParameterizedTypeName.get(ctx.traversalClassName, TypeVariableName.get("S"), TypeVariableName.get("E")));

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
                .addParameter(ctx.traversalSourceClassName, "traversalSource")
                .addStatement("super($N)", "traversalSource")
                .build());
        defaultTraversalClass.addMethod(MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PUBLIC)
                .addParameter(ctx.traversalSourceClassName, "traversalSource")
                .addParameter(ctx.graphTraversalAdminClassName, "traversal")
                .addStatement("super($N, $N.asAdmin())", "traversalSource", "traversal")
                .build());

        // add the override
        defaultTraversalClass.addMethod(MethodSpec.methodBuilder("iterate")
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(Override.class)
                .addStatement("return ($T) super.iterate()", ctx.traversalClassName)
                .returns(ParameterizedTypeName.get(ctx.traversalClassName, TypeVariableName.get("S"), TypeVariableName.get("E")))
                .build());
        defaultTraversalClass.addMethod(MethodSpec.methodBuilder("asAdmin")
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(Override.class)
                .addStatement("return ($T) super.asAdmin()", GraphTraversal.Admin.class)
                .returns(ParameterizedTypeName.get(ctx.graphTraversalAdminClassName, TypeVariableName.get("S"), TypeVariableName.get("E")))
                .build());
        defaultTraversalClass.addMethod(MethodSpec.methodBuilder("clone")
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(Override.class)
                .addStatement("return ($T) super.clone()", ctx.defaultTraversalClassName)
                .returns(ParameterizedTypeName.get(ctx.defaultTraversalClassName, TypeVariableName.get("S"), TypeVariableName.get("E")))
                .build());

        final JavaFile defaultTraversalJavaFile = JavaFile.builder(ctx.packageName, defaultTraversalClass.build()).build();
        defaultTraversalJavaFile.writeTo(filer);
    }

    private void generateTraversalInterface(final Context ctx) throws IOException {
        final TypeSpec.Builder traversalInterface = TypeSpec.interfaceBuilder(ctx.traversalClazz)
                .addModifiers(Modifier.PUBLIC)
                .addTypeVariables(Arrays.asList(TypeVariableName.get("S"), TypeVariableName.get("E")))
                .addSuperinterface(TypeName.get(ctx.annotatedDslType.asType()));

        // process the methods of the GremlinDsl annotated class
        for (ExecutableElement templateMethod : findMethodsOfElement(ctx.annotatedDslType, null)) {
            traversalInterface.addMethod(constructMethod(templateMethod, ctx.traversalClassName, ctx.dslName,
                    Modifier.PUBLIC, Modifier.DEFAULT));
        }

        // process the methods of GraphTraversal
        final TypeElement graphTraversalElement = elementUtils.getTypeElement(GraphTraversal.class.getCanonicalName());
        final Predicate<ExecutableElement> ignore = e -> e.getSimpleName().contentEquals("asAdmin") || e.getSimpleName().contentEquals("iterate");
        for (ExecutableElement templateMethod : findMethodsOfElement(graphTraversalElement, ignore)) {
            traversalInterface.addMethod(constructMethod(templateMethod, ctx.traversalClassName, ctx.dslName,
                    Modifier.PUBLIC, Modifier.DEFAULT));
        }

        // there are weird things with generics that require this method to be implemented if it isn't already present
        // in the GremlinDsl annotated class extending from GraphTraversal
        traversalInterface.addMethod(MethodSpec.methodBuilder("iterate")
                .addModifiers(Modifier.PUBLIC, Modifier.DEFAULT)
                .addAnnotation(Override.class)
                .addStatement("$T.super.iterate()", ClassName.get(ctx.annotatedDslType))
                .addStatement("return this")
                .returns(ParameterizedTypeName.get(ctx.traversalClassName, TypeVariableName.get("S"), TypeVariableName.get("E")))
                .build());

        final JavaFile traversalJavaFile = JavaFile.builder(ctx.packageName, traversalInterface.build()).build();
        traversalJavaFile.writeTo(filer);
    }

    private MethodSpec constructMethod(final Element element, final ClassName returnClazz, final String parent,
                                       final Modifier... modifiers) {
        final ExecutableElement templateMethod = (ExecutableElement) element;
        final String methodName = templateMethod.getSimpleName().toString();

        final TypeName returnType = getReturnTypeDefinition(returnClazz, templateMethod);
        final MethodSpec.Builder methodToAdd = MethodSpec.methodBuilder(methodName)
                .addModifiers(modifiers)
                .addAnnotation(Override.class)
                .addExceptions(templateMethod.getThrownTypes().stream().map(TypeName::get).collect(Collectors.toList()))
                .returns(returnType);

        templateMethod.getTypeParameters().forEach(tp -> methodToAdd.addTypeVariable(TypeVariableName.get(tp)));

        final String parentCall = parent.isEmpty() ? "" : parent + ".";
        final String body = "return ($T) " + parentCall + "super.$L(";
        addMethodBody(methodToAdd, templateMethod, body, ")", returnClazz, methodName);

        return methodToAdd.build();
    }

    private void addMethodBody(final MethodSpec.Builder methodToAdd, final ExecutableElement templateMethod,
                               final String startBody, final String endBody, final Object... statementArgs) {
        final List<? extends VariableElement> parameters = templateMethod.getParameters();
        final StringBuilder body = new StringBuilder(startBody);

        final int numberOfParams = parameters.size();
        for (int ix = 0; ix < numberOfParams; ix++) {
            final VariableElement param = parameters.get(ix);
            methodToAdd.addParameter(ParameterSpec.get(param));
            body.append(param.getSimpleName());
            if (ix < numberOfParams - 1) {
                body.append(",");
            }
        }

        body.append(endBody);

        // treat a final array as a varargs param
        if (!parameters.isEmpty() && parameters.get(parameters.size() - 1).asType().getKind() == TypeKind.ARRAY)
            methodToAdd.varargs(true);

        methodToAdd.addStatement(body.toString(), statementArgs);
    }

    private TypeName getOverridenReturnTypeDefinition(final ClassName returnClazz, final String[] typeValues) {
        return ParameterizedTypeName.get(returnClazz, Stream.of(typeValues).map(tv -> {
            try {
                return ClassName.get(Class.forName(tv));
            } catch (ClassNotFoundException cnfe) {
                if (tv.contains("extends")) {
                    final String[] sides = tv.toString().split(" extends ");
                    final TypeVariableName name = TypeVariableName.get(sides[0]);
                    try {
                        name.withBounds(ClassName.get(Class.forName(sides[1])));
                    } catch (Exception ex) {
                        name.withBounds(TypeVariableName.get(sides[1]));
                    }
                    return name;
                } else {
                    return TypeVariableName.get(tv);
                }
            }
        }).collect(Collectors.toList()).toArray(new TypeName[typeValues.length]));
    }

    private TypeName getReturnTypeDefinition(final ClassName returnClazz, final ExecutableElement templateMethod) {
        final List<? extends TypeMirror> returnTypeArguments = getTypeArguments(templateMethod);

        // build a return type with appropriate generic declarations (if such declarations are present)
        return returnTypeArguments.isEmpty() ?
                returnClazz :
                ParameterizedTypeName.get(returnClazz, returnTypeArguments.stream().map(TypeName::get).collect(Collectors.toList()).toArray(new TypeName[returnTypeArguments.size()]));
    }

    private void validateDSL(final Element dslElement) throws ProcessorException {
        if (dslElement.getKind() != ElementKind.INTERFACE)
            throw new ProcessorException(dslElement, "Only interfaces can be annotated with @%s", GremlinDsl.class.getSimpleName());

        final TypeElement typeElement = (TypeElement) dslElement;
        if (!typeElement.getModifiers().contains(Modifier.PUBLIC))
            throw new ProcessorException(dslElement, "The interface %s is not public.", typeElement.getQualifiedName());
    }

    private List<ExecutableElement> findMethodsOfElement(final Element element, final Predicate<ExecutableElement> ignore) {
        final Predicate<ExecutableElement> test = null == ignore ? ee -> false : ignore;
        return element.getEnclosedElements().stream()
                .filter(ee -> ee.getKind() == ElementKind.METHOD)
                .map(ee -> (ExecutableElement) ee)
                .filter(ee -> !test.test(ee)).collect(Collectors.toList());
    }

    private List<? extends TypeMirror> getTypeArguments(final ExecutableElement templateMethod) {
        final DeclaredType returnTypeMirror = (DeclaredType) templateMethod.getReturnType();
        return returnTypeMirror.getTypeArguments();
    }

    private class Context {
        private final TypeElement annotatedDslType;
        private final String packageName;
        private final String dslName;
        private final String traversalClazz;
        private final ClassName traversalClassName;
        private final String traversalSourceClazz;
        private final ClassName traversalSourceClassName;
        private final String defaultTraversalClazz;
        private final ClassName defaultTraversalClassName;
        private final ClassName graphTraversalAdminClassName;
        private final TypeElement traversalSourceDslType;
        private final boolean generateDefaultMethods;

        public Context(final TypeElement dslElement) {
            annotatedDslType = dslElement;

            // gets the annotation on the dsl class/interface
            GremlinDsl gremlinDslAnnotation = dslElement.getAnnotation(GremlinDsl.class);
            generateDefaultMethods = gremlinDslAnnotation.generateDefaultMethods();

            traversalSourceDslType = elementUtils.getTypeElement(gremlinDslAnnotation.traversalSource());
            packageName = getPackageName(dslElement, gremlinDslAnnotation);

            // create the Traversal implementation interface
            dslName = dslElement.getSimpleName().toString();
            final String dslPrefix = dslName.substring(0, dslName.length() - "TraversalDSL".length()); // chop off "TraversalDSL"
            traversalClazz = dslPrefix + "Traversal";
            traversalClassName = ClassName.get(packageName, traversalClazz);
            traversalSourceClazz = dslPrefix + "TraversalSource";
            traversalSourceClassName = ClassName.get(packageName, traversalSourceClazz);
            defaultTraversalClazz = "Default" + traversalClazz;
            defaultTraversalClassName = ClassName.get(packageName, defaultTraversalClazz);
            graphTraversalAdminClassName = ClassName.get(GraphTraversal.Admin.class);
        }

        private String getPackageName(final Element dslElement, final GremlinDsl gremlinDslAnnotation) {
            return gremlinDslAnnotation.packageName().isEmpty() ?
                    elementUtils.getPackageOf(dslElement).getQualifiedName().toString() :
                    gremlinDslAnnotation.packageName();
        }
    }
}
