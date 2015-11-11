/*
 * Copyright (c) 2015, Ravi Chodavarapu (rchodava@gmail.com)
 *
 * Parts of this are based on JGit, which has the following notes:
 *
 * Copyright (C) 2011, Google Inc.
 * and other copyright owners as documented in the project's IP log.
 *
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Distribution License v1.0 which
 * accompanies this distribution, is reproduced below, and is
 * available at http://www.eclipse.org/org/documents/edl-v10.php
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * - Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 *
 * - Redistributions in binary form must reproduce the above
 *   copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided
 *   with the distribution.
 *
 * - Neither the name of the Eclipse Foundation, Inc. nor the
 *   names of its contributors may be used to endorse or promote
 *   products derived from this software without specific prior
 *   written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
 * CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.chodavarapu.jgitaws.jgit;

import org.chodavarapu.jgitaws.JGitAwsConfiguration;
import org.eclipse.jgit.internal.storage.dfs.DfsRefDatabase;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.util.RefList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Ravi Chodavarapu (rchodava@gmail.com)
 */
public class DynamoRefDatabase extends DfsRefDatabase {
    private static final Logger logger = LoggerFactory.getLogger(DynamoRefDatabase.class);
    private final JGitAwsConfiguration configuration;

    public DynamoRefDatabase(AmazonRepository repository, JGitAwsConfiguration configuration) {
        super(repository);

        this.configuration = configuration;
    }

    @Override
    protected boolean compareAndPut(Ref oldRef, Ref newRef) throws IOException {
        return configuration.getRefRepository().compareAndPut(getRepository().getRepositoryName(), oldRef, newRef)
                .toBlocking()
                .lastOrDefault(false);
    }

    @Override
    protected boolean compareAndRemove(Ref oldRef) throws IOException {
        return configuration.getRefRepository().compareAndRemove(getRepository().getRepositoryName(), oldRef)
                .toBlocking()
                .lastOrDefault(false);
    }

    @Override
    protected AmazonRepository getRepository() {
        return (AmazonRepository) super.getRepository();
    }

    @Override
    protected RefCache scanAllRefs() throws IOException {
        logger.debug("Retrieving refs for repository {}", getRepository().getRepositoryName());

        RefCache cache = configuration.getRefRepository().getAllRefsSorted(getRepository().getRepositoryName())
                .toList()
                .map(refs -> {
                    RefList.Builder<Ref> allRefs = new RefList.Builder<>();
                    RefList.Builder<Ref> onlySymbolicRefs = new RefList.Builder<>();

                    for (Ref ref : refs) {
                        allRefs.add(ref);

                        if (ref.isSymbolic())
                            onlySymbolicRefs.add(ref);
                    }

                    return new RefCache(allRefs.toRefList(), onlySymbolicRefs.toRefList());
                })
                .toBlocking()
                .lastOrDefault(new RefCache(RefList.emptyList(), RefList.emptyList()));

        logger.debug("Retrieved {} refs for repository {}", cache.size(), getRepository().getRepositoryName());
        return cache;
    }
}
