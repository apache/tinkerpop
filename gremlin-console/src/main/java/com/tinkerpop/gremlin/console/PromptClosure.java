package com.tinkerpop.gremlin.console;

import groovy.lang.Closure;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PromptClosure extends Closure {

    private final String inputPrompt;

    public PromptClosure(final Object owner, final String inputPrompt) {
        super(owner);
        this.inputPrompt = inputPrompt;
    }

    public Object call() {
        return this.inputPrompt;
    }
}