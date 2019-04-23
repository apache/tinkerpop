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
package org.apache.tinkerpop.machine.species;

import org.apache.tinkerpop.machine.Machine;
import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.bytecode.BytecodeUtil;
import org.apache.tinkerpop.machine.bytecode.SourceInstruction;
import org.apache.tinkerpop.machine.bytecode.compiler.Compilation;
import org.apache.tinkerpop.machine.bytecode.compiler.SourceCompilation;
import org.apache.tinkerpop.machine.traverser.Traverser;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class LocalMachine implements Machine {

    static final String WITH_SOURCE_CODE = "tp:withSourceCode";
    final Map<UUID, SourceCompilation<?>> sources = new ConcurrentHashMap<>();

    private LocalMachine() {
        // use open();
    }

    @Override
    public <C> Bytecode<C> register(Bytecode<C> sourceCode) {
        sourceCode = sourceCode.clone(); // if the connection is local, don't mutate original
        final Optional<UUID> id = LocalMachine.getSourceId(sourceCode);
        if (id.isPresent()) {
            if (this.sources.containsKey(id.get())) {
                if (1 == sourceCode.getSourceInstructions().size())
                    return sourceCode;
                final SourceCompilation<C> source = (SourceCompilation<C>) this.sources.get(id.get());
                BytecodeUtil.mergeSourceInstructions(source.getSourceCode(), sourceCode);
            }
            sourceCode.getInstructions().removeIf(i -> i.op().equals(WITH_SOURCE_CODE));
        }
        final UUID uuid = UUID.randomUUID();
        this.sources.put(uuid, new SourceCompilation<>(sourceCode));
        final Bytecode<C> registeredBytecode = new Bytecode<>();
        registeredBytecode.addSourceInstruction(WITH_SOURCE_CODE, uuid.toString());
        registeredBytecode.getInstructions().addAll(sourceCode.getInstructions()); // all bytecode is returned (though, typically, this will be empty)
        return registeredBytecode;
    }

    @Override
    public <C> void unregister(final Bytecode<C> sourceCode) {
        LocalMachine.getSourceId(sourceCode).ifPresent(this.sources::remove);
    }

    @Override
    public <C, E> Iterator<Traverser<C, E>> submit(Bytecode<C> bytecode) {
        bytecode = bytecode.clone();
        final UUID sourceId = LocalMachine.getSourceId(bytecode).orElse(null);
        final SourceCompilation<C> source = (SourceCompilation<C>) this.sources.get(sourceId);
        return null == source ?
                Compilation.<C, Object, E>compile(bytecode).getProcessor().iterator(Collections.emptyIterator()) :
                Compilation.<C, Object, E>compile(source, bytecode).getProcessor().iterator(Collections.emptyIterator());
    }

    @Override
    public void close() {
        this.sources.clear();
    }

    public static Machine open() {
        return new LocalMachine();
    }

    private static <C> Optional<UUID> getSourceId(final Bytecode<C> bytecode) {
        for (final SourceInstruction sourceInstruction : bytecode.getSourceInstructions()) {
            if (sourceInstruction.op().equals(WITH_SOURCE_CODE))
                return Optional.of(UUID.fromString((String) sourceInstruction.args()[0]));
        }
        return Optional.empty();
    }
}
