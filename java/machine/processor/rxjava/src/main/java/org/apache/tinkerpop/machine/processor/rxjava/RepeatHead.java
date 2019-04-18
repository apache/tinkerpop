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
package org.apache.tinkerpop.machine.processor.rxjava;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import org.apache.tinkerpop.machine.function.branch.RepeatBranch;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.traverser.TraverserSet;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class RepeatHead<C, S> implements Processor<Traverser<C, S>, Traverser<C, S>>, FlowableTransformer<Traverser<C, S>, Traverser<C, S>> {

    private final TraverserSet<C, S> starts = new TraverserSet<>();
    private final RepeatBranch<C, S> repeatBranch;
    private Publisher<Traverser<C, S>> source;
    private Subscription upstream;
    private Subscriber<? super Traverser<C, S>> downstream;
    private RepeatTail<C, S> repeatTail;

    RepeatHead(final RepeatBranch<C, S> repeatBranch) {
        this.repeatBranch = repeatBranch;
    }

    void setRepeatTail(final RepeatTail<C, S> repeatTail) {
        this.repeatTail = repeatTail;
    }


    @Override
    public void subscribe(final Subscriber<? super Traverser<C, S>> subscriber) {
        this.downstream = subscriber;
        this.source.subscribe(this);
        Subscription subscription = new Subscription() {
            @Override
            public void request(long l) {
                upstream.request(l);
                drain();
            }

            @Override
            public void cancel() {
                upstream.cancel();
            }
        };
        this.downstream.onSubscribe(subscription);
    }

    void tailComplete() {
        drain();
        downstream.onComplete();
    }

    @Override
    public void onSubscribe(final Subscription subscription) {
        this.upstream = subscription;

    }

    void addTraverser(final Traverser<C, S> traverser) {
        this.starts.add(traverser);
    }

    @Override
    public void onNext(final Traverser<C, S> traverser) {
        this.processTraverser(traverser);
    }

    @Override
    public void onError(Throwable throwable) {
        this.downstream.onError(throwable);
    }

    @Override
    public void onComplete() {
        this.upstream.cancel();
    }

    private void drain() {
        while (!this.starts.isEmpty()) {
            final Traverser<C, S> traverser = this.starts.remove();
            this.processTraverser(traverser);
        }
    }

    private void processTraverser(final Traverser<C, S> traverser) {
        if (this.repeatBranch.hasStartPredicates()) {
            if (1 == this.repeatBranch.getUntilLocation()) {
                if (this.repeatBranch.getUntil().filterTraverser(traverser)) {
                    this.repeatTail.onNextSkip(traverser.repeatDone(this.repeatBranch));
                } else if (2 == this.repeatBranch.getEmitLocation() && this.repeatBranch.getEmit().filterTraverser(traverser)) {
                    this.downstream.onNext(traverser);
                    this.repeatTail.onNextSkip(traverser.repeatDone(this.repeatBranch));
                } else
                    this.downstream.onNext(traverser);
            } else if (1 == this.repeatBranch.getEmitLocation()) {
                if (this.repeatBranch.getEmit().filterTraverser(traverser))
                    this.repeatTail.onNextSkip(traverser.repeatDone(this.repeatBranch));
                if (2 == this.repeatBranch.getUntilLocation() && this.repeatBranch.getUntil().filterTraverser(traverser))
                    this.repeatTail.onNextSkip(traverser.repeatDone(this.repeatBranch));
                else
                    this.downstream.onNext(traverser);
            }
        } else
            this.downstream.onNext(traverser);
    }

    @Override
    public Publisher<Traverser<C, S>> apply(final Flowable<Traverser<C, S>> flowable) {
        this.source = flowable;
        return this;
    }
}
