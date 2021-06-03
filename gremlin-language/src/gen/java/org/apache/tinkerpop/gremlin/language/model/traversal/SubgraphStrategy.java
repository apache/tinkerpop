package org.example.org.apache.tinkerpop.gremlin.language.model.traversal;

public abstract class SubgraphStrategy {
    private SubgraphStrategy() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    /**
     * An interface for applying a function to a SubgraphStrategy according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(Vertices instance) ;
        
        R visit(Edges instance) ;
        
        R visit(VertexProperties instance) ;
        
        R visit(CheckAdjacentVertixes instance) ;
    }
    
    /**
     * An interface for applying a function to a SubgraphStrategy according to its variant (subclass). If a visit() method
     * for a particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(SubgraphStrategy instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        @Override
        default R visit(Vertices instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(Edges instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(VertexProperties instance) {
            return otherwise(instance);
        }
        
        @Override
        default R visit(CheckAdjacentVertixes instance) {
            return otherwise(instance);
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
     */
    public static final class Vertices extends SubgraphStrategy {
        public final NestedTraversal vertices;
        
        /**
         * Constructs an immutable Vertices object
         */
        public Vertices(NestedTraversal vertices) {
            this.vertices = vertices;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Vertices)) {
                return false;
            }
            Vertices o = (Vertices) other;
            return vertices.equals(o.vertices);
        }
        
        @Override
        public int hashCode() {
            return 2 * vertices.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
     */
    public static final class Edges extends SubgraphStrategy {
        public final NestedTraversal edges;
        
        /**
         * Constructs an immutable Edges object
         */
        public Edges(NestedTraversal edges) {
            this.edges = edges;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Edges)) {
                return false;
            }
            Edges o = (Edges) other;
            return edges.equals(o.edges);
        }
        
        @Override
        public int hashCode() {
            return 2 * edges.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
     */
    public static final class VertexProperties extends SubgraphStrategy {
        public final NestedTraversal vertexProperties;
        
        /**
         * Constructs an immutable VertexProperties object
         */
        public VertexProperties(NestedTraversal vertexProperties) {
            this.vertexProperties = vertexProperties;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof VertexProperties)) {
                return false;
            }
            VertexProperties o = (VertexProperties) other;
            return vertexProperties.equals(o.vertexProperties);
        }
        
        @Override
        public int hashCode() {
            return 2 * vertexProperties.hashCode();
        }
    }
    
    /**
     * @type boolean
     */
    public static final class CheckAdjacentVertixes extends SubgraphStrategy {
        public final Boolean checkAdjacentVertixes;
        
        /**
         * Constructs an immutable CheckAdjacentVertixes object
         */
        public CheckAdjacentVertixes(Boolean checkAdjacentVertixes) {
            this.checkAdjacentVertixes = checkAdjacentVertixes;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof CheckAdjacentVertixes)) {
                return false;
            }
            CheckAdjacentVertixes o = (CheckAdjacentVertixes) other;
            return checkAdjacentVertixes.equals(o.checkAdjacentVertixes);
        }
        
        @Override
        public int hashCode() {
            return 2 * checkAdjacentVertixes.hashCode();
        }
    }
}
