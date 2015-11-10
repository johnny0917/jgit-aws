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
package org.chodavarapu.jgitaws;

import com.amazonaws.RequestClientOptions;
import com.amazonaws.services.s3.AmazonS3;
import org.chodavarapu.jgitaws.aws.DynamoClient;
import org.chodavarapu.jgitaws.repositories.ConfigurationRepository;
import org.chodavarapu.jgitaws.repositories.PackDescriptionRepository;
import org.chodavarapu.jgitaws.repositories.PackRepository;
import org.chodavarapu.jgitaws.repositories.RefRepository;

/**
 * @author Ravi Chodavarapu (rchodava@gmail.com)
 */
public class JGitAwsConfiguration {
    public static final int DEFAULT_STREAMING_BLOCK_SIZE = RequestClientOptions.DEFAULT_STREAM_BUFFER_SIZE;
    public static final String DEFAULT_CONFIGURATIONS_TABLE_NAME = "jga.Configurations";
    public static final String DEFAULT_REFS_TABLE_NAME = "jga.Refs";
    public static final String DEFAULT_PACK_DESCRIPTIONS_TABLE_NAME = "jga.PackDescriptions";
    public static final String DEFAULT_PACKS_BUCKET_NAME = "jga.Packs";

    private final DynamoClient dynamoClient;
    private final AmazonS3 s3Client;
    private final ConfigurationRepository configurationRepository;
    private final PackRepository packRepository;
    private final PackDescriptionRepository packDescriptionRepository;
    private final RefRepository refRepository;

    private String configurationsTableName = DEFAULT_CONFIGURATIONS_TABLE_NAME;
    private String packDescriptionsTableName = DEFAULT_PACK_DESCRIPTIONS_TABLE_NAME;
    private String packsBucketName = DEFAULT_PACKS_BUCKET_NAME;
    private String refsTableName = DEFAULT_REFS_TABLE_NAME;
    private int streamingBlockSize = DEFAULT_STREAMING_BLOCK_SIZE;

    public JGitAwsConfiguration(DynamoClient dynamoClient, AmazonS3 s3Client) {
        this.dynamoClient = dynamoClient;
        this.s3Client = s3Client;

        this.configurationRepository = new ConfigurationRepository(this);
        this.packRepository = new PackRepository(this);
        this.packDescriptionRepository = new PackDescriptionRepository(this);
        this.refRepository = new RefRepository(this);
    }

    public int getStreamingBlockSize() {
        return streamingBlockSize;
    }

    public void setStreamingBlockSize(int streamingBlockSize) {
        this.streamingBlockSize = streamingBlockSize;
    }

    public String getPacksBucketName() {
        return packsBucketName;
    }

    public void setPacksBucketName(String packsBucketName) {
        this.packsBucketName = packsBucketName;
    }

    public PackRepository getPackRepository() {
        return packRepository;
    }

    public ConfigurationRepository getConfigurationRepository() {
        return configurationRepository;
    }

    public PackDescriptionRepository getPackDescriptionRepository() {
        return packDescriptionRepository;
    }

    public RefRepository getRefRepository() {
        return refRepository;
    }

    public DynamoClient getDynamoClient() {
        return dynamoClient;
    }

    public String getConfigurationsTableName() {
        return configurationsTableName;
    }

    public JGitAwsConfiguration setConfigurationsTableName(String name) {
        this.configurationsTableName = name;
        return this;
    }

    public String getPackDescriptionsTableName() {
        return packDescriptionsTableName;
    }

    public JGitAwsConfiguration setPackDescriptionsTableName(String packDescriptionsTableName) {
        this.packDescriptionsTableName = packDescriptionsTableName;
        return this;
    }

    public String getRefsTableName() {
        return refsTableName;
    }

    public JGitAwsConfiguration setRefsTableName(String name) {
        this.configurationsTableName = name;
        return this;
    }

    public AmazonS3 getS3Client() {
        return s3Client;
    }
}
