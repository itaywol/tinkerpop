package com.tinkerpop.gremlin.olap;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.GraphMemory;
import com.tinkerpop.blueprints.computer.MessageType;
import com.tinkerpop.blueprints.computer.Messenger;
import com.tinkerpop.blueprints.computer.VertexProgram;
import com.tinkerpop.blueprints.query.util.HasContainer;
import com.tinkerpop.blueprints.util.StreamFactory;
import com.tinkerpop.gremlin.GremlinJ;
import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.PathHolder;
import com.tinkerpop.gremlin.Pipe;
import com.tinkerpop.gremlin.SimpleHolder;
import com.tinkerpop.gremlin.oltp.map.GraphQueryPipe;
import com.tinkerpop.gremlin.util.optimizers.HolderOptimizer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinVertexProgram<M extends GremlinMessage> implements VertexProgram<M> {

    private MessageType.Global global = MessageType.Global.of(GREMLIN_MESSAGE);

    protected static final String GREMLIN_MESSAGE = "gremlinMessage";
    private static final String GREMLIN_PIPELINE = "gremlinPipeline";
    private static final String VOTE_TO_HALT = "voteToHalt";
    public static final String TRACK_PATHS = "trackPaths";
    // TODO: public static final String MESSAGES_SENT = "messagesSent";
    public static final String GREMLIN_TRACKER = "gremlinTracker";
    private final Supplier<GremlinJ> gremlinSupplier;

    private GremlinVertexProgram(final Supplier<GremlinJ> gremlinSupplier) {
        this.gremlinSupplier = gremlinSupplier;
    }

    public void setup(final GraphMemory graphMemory) {
        graphMemory.setIfAbsent(GREMLIN_PIPELINE, this.gremlinSupplier);
        graphMemory.setIfAbsent(VOTE_TO_HALT, true);
        graphMemory.setIfAbsent(TRACK_PATHS, HolderOptimizer.trackPaths(this.gremlinSupplier.get()));
    }

    public void execute(final Vertex vertex, final Messenger<M> messenger, final GraphMemory graphMemory) {
        if (graphMemory.isInitialIteration()) {
            executeFirstIteration(vertex, messenger, graphMemory);
        } else {
            executeOtherIterations(vertex, messenger, graphMemory);
        }
    }

    private void executeFirstIteration(final Vertex vertex, final Messenger<M> messenger, final GraphMemory graphMemory) {
        final GremlinJ gremlin = graphMemory.<Supplier<GremlinJ>>get(GREMLIN_PIPELINE).get();
        if (null != graphMemory.getReductionMemory())
            gremlin.addPipe(new ReductionPipe(gremlin, graphMemory.getReductionMemory()));
        // the head is always an IdentityPipe so simply skip it
        final GraphQueryPipe graphQueryPipe = (GraphQueryPipe) gremlin.getPipes().get(1);
        final String future = (gremlin.getPipes().size() == 2) ? Holder.NO_FUTURE : ((Pipe) gremlin.getPipes().get(2)).getAs();

        final AtomicBoolean voteToHalt = new AtomicBoolean(true);
        final List<HasContainer> hasContainers = graphQueryPipe.queryBuilder.hasContainers;
        if (graphQueryPipe.returnClass.equals(Vertex.class) && HasContainer.testAll(vertex, hasContainers)) {
            final Holder<Vertex> holder = graphMemory.<Boolean>get(TRACK_PATHS) ?
                    new PathHolder<>(graphQueryPipe.getAs(), vertex) :
                    new SimpleHolder<>(vertex);
            holder.setFuture(future);
            messenger.sendMessage(vertex, MessageType.Global.of(GREMLIN_MESSAGE, vertex), GremlinMessage.of(holder));
            voteToHalt.set(false);
        } else if (graphQueryPipe.returnClass.equals(Edge.class)) {
            StreamFactory.stream(vertex.query().direction(Direction.OUT).edges())
                    .filter(edge -> HasContainer.testAll(edge, hasContainers))
                    .forEach(e -> {
                        final Holder<Edge> holder = graphMemory.<Boolean>get(TRACK_PATHS) ?
                                new PathHolder<>(graphQueryPipe.getAs(), e) :
                                new SimpleHolder<>(e);
                        holder.setFuture(future);
                        messenger.sendMessage(vertex, MessageType.Global.of(GREMLIN_MESSAGE, vertex), GremlinMessage.of(holder));
                        voteToHalt.set(false);
                    });
        }
        graphMemory.and(VOTE_TO_HALT, voteToHalt.get());
    }

    private void executeOtherIterations(final Vertex vertex, final Messenger<M> messenger, final GraphMemory graphMemory) {
        final GremlinJ gremlin = graphMemory.<Supplier<GremlinJ>>get(GREMLIN_PIPELINE).get();
        if (null != graphMemory.getReductionMemory())
            gremlin.addPipe(new ReductionPipe(gremlin, graphMemory.getReductionMemory()));
        if (graphMemory.<Boolean>get(TRACK_PATHS)) {
            final GremlinPaths tracker = new GremlinPaths(vertex);
            graphMemory.and(VOTE_TO_HALT, GremlinPathMessage.execute(vertex, (Iterable) messenger.receiveMessages(vertex, this.global), messenger, tracker, gremlin));
            vertex.setProperty(GREMLIN_TRACKER, tracker);
        } else {
            final GremlinCounters tracker = new GremlinCounters(vertex);
            graphMemory.and(VOTE_TO_HALT, GremlinCounterMessage.execute(vertex, (Iterable) messenger.receiveMessages(vertex, this.global), messenger, tracker, gremlin));
            vertex.setProperty(GREMLIN_TRACKER, tracker);
        }
    }

    ////////// GRAPH COMPUTER METHODS

    public boolean terminate(final GraphMemory graphMemory) {
        final boolean voteToHalt = graphMemory.get(VOTE_TO_HALT);
        if (voteToHalt) {
            return true;
        } else {
            graphMemory.or(VOTE_TO_HALT, true);
            return false;
        }
    }

    public Map<String, KeyType> getComputeKeys() {
        return VertexProgram.ofComputeKeys(GREMLIN_TRACKER, KeyType.VARIABLE);
    }

    public static Builder create() {
        return new Builder();
    }

    public static class Builder {
        private Supplier<GremlinJ> gremlin;

        public Builder gremlin(final Supplier<GremlinJ> gremlin) {
            this.gremlin = gremlin;
            return this;
        }

        public GremlinVertexProgram build() {
            return new GremlinVertexProgram(this.gremlin);
        }
    }
}