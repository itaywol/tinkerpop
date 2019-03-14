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
package org.apache.tinkerpop.machine.pipes;

import org.apache.tinkerpop.machine.bytecode.Compilation;
import org.apache.tinkerpop.machine.functions.BranchFunction;
import org.apache.tinkerpop.machine.functions.branch.selector.Selector;
import org.apache.tinkerpop.machine.traversers.Traverser;
import org.apache.tinkerpop.util.MultiIterator;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class BranchStep<C, S, E, M> extends AbstractStep<C, S, E> {

    private final Selector<C, S, M> branchSelector;
    private final Map<M, List<Compilation<C, S, E>>> branches;
    private Iterator<Traverser<C, E>> output = Collections.emptyIterator();

    public BranchStep(final AbstractStep<C, ?, S> previousStep, final BranchFunction<C, S, E, M> branchFunction) {
        super(previousStep, branchFunction);
        this.branchSelector = branchFunction.getBranchSelector();
        this.branches = branchFunction.getBranches();
    }

    @Override
    public boolean hasNext() {
        this.stageOutput();
        return this.output.hasNext();
    }

    @Override
    public Traverser<C, E> next() {
        this.stageOutput();
        return this.output.next();
    }

    private final void stageOutput() {
        while (!this.output.hasNext() && super.hasNext()) {
            final Traverser<C, S> traverser = super.getPreviousTraverser();
            final Optional<M> token = this.branchSelector.from(traverser);
            if (token.isPresent()) {
                final List<Compilation<C, S, E>> matches = this.branches.get(token.get());
                if (1 == matches.size())
                    this.output = matches.get(0).addTraverser(traverser.clone());
                else {
                    this.output = new MultiIterator<>();
                    for (final Compilation<C, S, E> branch : matches) {
                        ((MultiIterator<Traverser<C, E>>) this.output).addIterator(branch.addTraverser(traverser.clone()));
                    }
                }
            }
        }
    }

}