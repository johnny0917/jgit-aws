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

import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.*;
import org.chodavarapu.jgitaws.JGitAwsConfiguration;
import rx.Observable;

/**
 * @author Ravi Chodavarapu (rchodava@gmail.com)
 */
public class ConfigurationRepository {
    private static final String REPOSITORY_NAME_ATTRIBUTE = "RepositoryName";
    private static final String TEXT_ATTRIBUTE = "Text";

    private final JGitAwsConfiguration configuration;

    public ConfigurationRepository(JGitAwsConfiguration configuration) {
        this.configuration = configuration;
    }

    public Observable<String> getConfiguration(String repositoryName) {
        return configuration.getDynamoClient().getItem(
                configuration.getConfigurationsTableName(),
                new PrimaryKey(
                        REPOSITORY_NAME_ATTRIBUTE,
                        repositoryName))
                .map(item -> item == null ? null : item.getString(TEXT_ATTRIBUTE));
    }

    public Observable<Void> updateConfiguration(String repositoryName, String text) {
        return configuration.getDynamoClient().updateItem(configuration.getConfigurationsTableName(),
                new UpdateItemSpec()
                        .withPrimaryKey(REPOSITORY_NAME_ATTRIBUTE, repositoryName)
                        .withAttributeUpdate(new AttributeUpdate(TEXT_ATTRIBUTE).put(text)),
                () -> new CreateTableRequest()
                        .withTableName(configuration.getConfigurationsTableName())
                        .withKeySchema(
                                new KeySchemaElement()
                                        .withAttributeName(REPOSITORY_NAME_ATTRIBUTE)
                                        .withKeyType(KeyType.HASH))
                        .withAttributeDefinitions(
                                new AttributeDefinition()
                                        .withAttributeName(REPOSITORY_NAME_ATTRIBUTE)
                                        .withAttributeType(ScalarAttributeType.S),
                                new AttributeDefinition()
                                        .withAttributeName(TEXT_ATTRIBUTE)
                                        .withAttributeType(ScalarAttributeType.S)));
    }
}
