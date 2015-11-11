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
package org.chodavarapu.jgitaws.repositories;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.*;
import org.chodavarapu.jgitaws.JGitAwsConfiguration;
import org.chodavarapu.jgitaws.jgit.PipedDfsOutputStream;
import org.chodavarapu.jgitaws.jgit.S3ObjectReadableChannel;
import org.eclipse.jgit.internal.storage.dfs.DfsOutputStream;
import org.eclipse.jgit.internal.storage.dfs.DfsPackDescription;
import org.eclipse.jgit.internal.storage.dfs.ReadableChannel;
import org.eclipse.jgit.internal.storage.pack.PackExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.schedulers.Schedulers;
import rx.util.async.Async;

import java.io.IOException;
import java.io.PipedInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Ravi Chodavarapu (rchodava@gmail.com)
 */
public class PackRepository {
    private static final Logger logger = LoggerFactory.getLogger(PackRepository.class);
    private final JGitAwsConfiguration configuration;

    public PackRepository(JGitAwsConfiguration configuration) {
        this.configuration = configuration;
    }

    private String objectName(String repositoryName, String packName) {
        return new StringBuilder(repositoryName).append('/').append(packName).toString();
    }

    public Observable<Void> deletePacks(Collection<DfsPackDescription> packs) {
        List<String> objectNames = getObjectNames(packs);

        return Async.fromCallable(() -> configuration.getS3Client().deleteObjects(
                new DeleteObjectsRequest(configuration.getPacksBucketName())
                        .withKeys(objectNames.toArray(new String[objectNames.size()]))))
                .map(r -> null);
    }

    private List<String> getObjectNames(Collection<DfsPackDescription> packs) {
        List<String> objectNames = new ArrayList<>();
        for (DfsPackDescription pack : packs) {
            for (PackExt ext : PackExt.values()) {
                if (pack.hasFileExt(ext)) {
                    objectNames.add(objectName(
                            pack.getRepositoryDescription().getRepositoryName(),
                            pack.getFileName(ext)));
                }
            }
        }

        return objectNames;
    }

    public ReadableChannel readPack(String repositoryName, String packName) throws IOException {
        return new S3ObjectReadableChannel(configuration, objectName(repositoryName, packName));
    }

    public DfsOutputStream savePack(String repositoryName, String packName, long length) throws IOException {
        PipedInputStream pipedInputStream = new PipedInputStream(configuration.getStreamingBlockSize());

        ObjectMetadata metaData = new ObjectMetadata();
        metaData.setContentLength(length);

        String objectName = objectName(repositoryName, packName);

        Async.fromAction(() -> {
            logger.debug("Attempting to save pack {} to S3 bucket", objectName);
            try {
                configuration.getS3Client().putObject(
                        configuration.getPacksBucketName(), objectName, pipedInputStream, metaData);
            } catch (AmazonServiceException e) {
                if ("InvalidBucketName".equals(e.getErrorCode()) || "InvalidBucketState".equals(e.getErrorCode())) {
                    logger.debug("S3 packs bucket does not exist yet, creating it");
                    configuration.getS3Client().createBucket(new CreateBucketRequest(configuration.getPacksBucketName()));
                    configuration.getS3Client().setBucketVersioningConfiguration(
                            new SetBucketVersioningConfigurationRequest(
                                    configuration.getPacksBucketName(),
                                    new BucketVersioningConfiguration(BucketVersioningConfiguration.OFF)));

                    logger.debug("Created bucket, saving pack {}", objectName);
                    configuration.getS3Client().putObject(
                            configuration.getPacksBucketName(), objectName, pipedInputStream, metaData);
                } else {
                    throw e;
                }
            }
        }, null, Schedulers.io());

        return new PipedDfsOutputStream(
                pipedInputStream,
                objectName,
                (int) length,
                configuration.getStreamingBlockSize());
    }
}
