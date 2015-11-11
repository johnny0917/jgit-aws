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
import org.eclipse.jgit.internal.storage.dfs.*;
import org.eclipse.jgit.internal.storage.pack.PackExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author Ravi Chodavarapu (rchodava@gmail.com)
 */
public class S3WithDynamoMetaDataObjDatabase extends DfsObjDatabase {
    private static final Logger logger = LoggerFactory.getLogger(S3WithDynamoMetaDataObjDatabase.class);

    private final JGitAwsConfiguration configuration;
    private final SecureRandom random = new SecureRandom();

    public S3WithDynamoMetaDataObjDatabase(
            AmazonRepository repository,
            DfsReaderOptions readerOptions,
            JGitAwsConfiguration configuration) {
        super(repository, readerOptions);

        this.configuration = configuration;
    }

    @Override
    protected void commitPackImpl(Collection<DfsPackDescription> desc, Collection<DfsPackDescription> replaces) throws IOException {
        int totalCount = (desc == null ? 0 : desc.size()) + (replaces == null ? 0 : replaces.size());
        logger.debug("Committing {} packs for repository {}", totalCount,
                getRepository().getDescription().getRepositoryName());
        configuration.getPackDescriptionRepository().updatePackDescriptions(desc, replaces)
                .doOnCompleted(() -> logger.debug("Commit of {} packs to S3 complete!", totalCount)).toBlocking().last();
    }

    @Override
    protected List<DfsPackDescription> listPacks() throws IOException {
        logger.debug("Retrieving list of packs for repository {}", getRepository().getDescription().getRepositoryName());
        return configuration.getPackDescriptionRepository().getAllPackDescriptions((AmazonRepository) getRepository())
                .toList()
                .doOnNext(l -> logger.debug("Retrieved {} packs for repository {}",
                        l == null ? 0 : l.size(), getRepository().getDescription().getRepositoryName()))
                .toBlocking()
                .lastOrDefault(Collections.emptyList());
    }

    @Override
    protected ReadableChannel openFile(DfsPackDescription desc, PackExt ext) throws IOException {
        logger.debug("Reading pack file {} from S3 bucket", desc.getFileName(ext));
        return configuration.getPackRepository().readPack(
                desc.getRepositoryDescription().getRepositoryName(),
                desc.getFileName(ext));
    }

    @Override
    protected DfsPackDescription newPack(PackSource source) throws IOException {
        StringBuilder packName = new StringBuilder();
        switch (source) {
            case COMPACT: packName.append("cmp-"); break;
            case GC: packName.append("gc-"); break;
            case INSERT: packName.append("ins-"); break;
            case RECEIVE: packName.append("rec-"); break;
            case UNREACHABLE_GARBAGE: packName.append("ug-"); break;
        }
        packName.append(System.currentTimeMillis());
        packName.append('-');
        packName.append(random.nextInt(100));

        logger.debug("Created new pack file {}", packName);
        return new DfsPackDescription(getRepository().getDescription(), packName.toString());
    }

    @Override
    protected void rollbackPack(Collection<DfsPackDescription> descriptions) {
        if (logger.isDebugEnabled()) {
            if (descriptions != null) {
                for (DfsPackDescription desc : descriptions) {
                    StringBuilder files = new StringBuilder();
                    for (PackExt ext : PackExt.values()) {
                        if (desc.hasFileExt(ext)) {
                            files.append(desc.getFileName(ext));
                            files.append(", ");
                        }
                    }
                    logger.debug("Rolling back commit of pack files {}due to error", files.toString());
                }
            }
        }
        try {
            configuration.getPackRepository().deletePacks(descriptions).toBlocking().last();
        } catch (Exception e) {
            logger.debug("Error occurred while trying to rollback a pack commit operation!", e);
        }
    }

    @Override
    protected DfsOutputStream writeFile(DfsPackDescription desc, PackExt ext) throws IOException {
        logger.debug("Writing pack file {} to S3 bucket", desc.getFileName(ext));
        return configuration.getPackRepository().savePack(
                desc.getRepositoryDescription().getRepositoryName(),
                desc.getFileName(ext),
                desc.getFileSize(ext));
    }
}
