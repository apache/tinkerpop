package com.tinkerpop.gremlin.structure;

/**
 * This enumeration allows for the specification of the type of a {@link Property}.
 * Properties can either be their standard form, value form, hidden form, or hidden value form.
 * Note that this is different than a property class like {@link Property} or {@link VertexProperty}.
 * This enumeration is used to denote those aspects of a property that can not be realized by class alone.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum PropertyType {
    PROPERTY {
        @Override
        public final boolean forProperties() {
            return true;
        }

        @Override
        public final boolean forValues() {
            return false;
        }

        @Override
        public final boolean forHiddens() {
            return false;
        }

    }, VALUE {
        @Override
        public final boolean forProperties() {
            return false;
        }

        @Override
        public final boolean forValues() {
            return true;
        }

        @Override
        public final boolean forHiddens() {
            return false;
        }
    }, HIDDEN_PROPERTY {
        @Override
        public final boolean forProperties() {
            return true;
        }

        @Override
        public final boolean forValues() {
            return false;
        }

        @Override
        public final boolean forHiddens() {
            return true;
        }
    }, HIDDEN_VALUE {
        @Override
        public final boolean forProperties() {
            return false;
        }

        @Override
        public final boolean forValues() {
            return true;
        }

        @Override
        public final boolean forHiddens() {
            return true;
        }
    };

    public abstract boolean forProperties();
    public abstract boolean forValues();
    public abstract boolean forHiddens();
}
