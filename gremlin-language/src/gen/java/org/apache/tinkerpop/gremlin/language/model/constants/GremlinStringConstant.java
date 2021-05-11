package org.example.org.apache.tinkerpop.gremlin.language.model.constants;

public abstract class GremlinStringConstant {
    private GremlinStringConstant() {}
    
    public abstract <R> R accept(Visitor<R> visitor) ;
    
    /**
     * An interface for applying a function to a GremlinStringConstant according to its variant (subclass)
     */
    public interface Visitor<R> {
        R visit(WithOptions instance) ;
        
        R visit(ShortestPath instance) ;
        
        R visit(PageRank instance) ;
        
        R visit(PeerPressure instance) ;
    }
    
    /**
     * An interface for applying a function to a GremlinStringConstant according to its variant (subclass). If a visit()
     * method for a particular variant is not implemented, a default method is used instead.
     */
    public interface PartialVisitor<R> extends Visitor<R> {
        default R otherwise(GremlinStringConstant instance) {
            throw new IllegalStateException("Non-exhaustive patterns when matching: " + instance);
        }
        
        default R visit(WithOptions instance) {
            return otherwise(instance);
        }
        
        default R visit(ShortestPath instance) {
            return otherwise(instance);
        }
        
        default R visit(PageRank instance) {
            return otherwise(instance);
        }
        
        default R visit(PeerPressure instance) {
            return otherwise(instance);
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/constants.WithOptionsStringConstant
     */
    public static final class WithOptions extends GremlinStringConstant {
        public final WithOptionsStringConstant withOptions;
        
        /**
         * Constructs an immutable WithOptions object
         */
        public WithOptions(WithOptionsStringConstant withOptions) {
            this.withOptions = withOptions;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof WithOptions)) return false;
            WithOptions o = (WithOptions) other;
            return withOptions.equals(o.withOptions);
        }
        
        @Override
        public int hashCode() {
            return 2 * withOptions.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/constants.ShortestPathStringConstant
     */
    public static final class ShortestPath extends GremlinStringConstant {
        public final ShortestPathStringConstant shortestPath;
        
        /**
         * Constructs an immutable ShortestPath object
         */
        public ShortestPath(ShortestPathStringConstant shortestPath) {
            this.shortestPath = shortestPath;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ShortestPath)) return false;
            ShortestPath o = (ShortestPath) other;
            return shortestPath.equals(o.shortestPath);
        }
        
        @Override
        public int hashCode() {
            return 2 * shortestPath.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/constants.PageRankStringConstant
     */
    public static final class PageRank extends GremlinStringConstant {
        public final PageRankStringConstant pageRank;
        
        /**
         * Constructs an immutable PageRank object
         */
        public PageRank(PageRankStringConstant pageRank) {
            this.pageRank = pageRank;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof PageRank)) return false;
            PageRank o = (PageRank) other;
            return pageRank.equals(o.pageRank);
        }
        
        @Override
        public int hashCode() {
            return 2 * pageRank.hashCode();
        }
    }
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/constants.PeerPressureStringConstant
     */
    public static final class PeerPressure extends GremlinStringConstant {
        public final PeerPressureStringConstant peerPressure;
        
        /**
         * Constructs an immutable PeerPressure object
         */
        public PeerPressure(PeerPressureStringConstant peerPressure) {
            this.peerPressure = peerPressure;
        }
        
        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visit(this);
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof PeerPressure)) return false;
            PeerPressure o = (PeerPressure) other;
            return peerPressure.equals(o.peerPressure);
        }
        
        @Override
        public int hashCode() {
            return 2 * peerPressure.hashCode();
        }
    }
}
