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
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class RepeatTail<C, S> implements Processor<Traverser<C, S>, Traverser<C, S>>, FlowableTransformer<Traverser<C, S>, Traverser<C, S>> {

    private Publisher<Traverser<C, S>> source;
    private final RepeatBranch<C, S> repeatBranch;
    private Subscription upstream;
    private RepeatHead<C, S> repeatHead;
    private Subscriber<? super Traverser<C, S>> downstream;

    RepeatTail(final RepeatBranch<C, S> repeatBranch) {
        this.repeatBranch = repeatBranch;
    }

    void setRepeatHead(final RepeatHead<C, S> repeatHead) {
        this.repeatHead = repeatHead;
    }

    @Override
    public Publisher<Traverser<C, S>> apply(final Flowable<Traverser<C, S>> flowable) {
        this.source = flowable;
        return this;
    }


    @Override
    public void subscribe(final Subscriber<? super Traverser<C, S>> subscriber) {
        this.downstream = subscriber;
        this.source.subscribe(this);
        this.downstream.onSubscribe(new Subscription() {
            @Override
            public void request(long l) {
                upstream.request(l);
                repeatHead.tailComplete();
            }

            @Override
            public void cancel() {
                upstream.cancel();
            }
        });
    }

    @Override
    public void onSubscribe(final Subscription subscription) {
        this.upstream = subscription;

    }

    void onNextSkip(final Traverser<C,S> traverser) {
        this.downstream.onNext(traverser);
    }

    @Override
    public void onNext(final Traverser<C, S> traverser) {
        final Traverser<C, S> t = traverser.repeatLoop(this.repeatBranch);
        if (this.repeatBranch.hasEndPredicates()) {
            if (3 == this.repeatBranch.getUntilLocation()) {
                if (this.repeatBranch.getUntil().filterTraverser(t)) {
                    this.downstream.onNext(t.repeatDone(this.repeatBranch));
                } else if (4 == this.repeatBranch.getEmitLocation() && this.repeatBranch.getEmit().filterTraverser(t)) {
                    this.repeatHead.addTraverser(t);
                    this.downstream.onNext(t.repeatDone(this.repeatBranch));
                } else
                    this.repeatHead.addTraverser(t);
            } else if (3 == this.repeatBranch.getEmitLocation()) {
                if (this.repeatBranch.getEmit().filterTraverser(t))
                    this.downstream.onNext(t.repeatDone(this.repeatBranch));
                if (4 == this.repeatBranch.getUntilLocation() && this.repeatBranch.getUntil().filterTraverser(t))
                    this.downstream.onNext(t.repeatDone(this.repeatBranch));
                else
                    this.repeatHead.addTraverser(t);
            }
        } else {
            this.repeatHead.addTraverser(t);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        this.downstream.onError(throwable);
    }

    @Override
    public void onComplete() {
        this.downstream.onComplete();
    }

}
