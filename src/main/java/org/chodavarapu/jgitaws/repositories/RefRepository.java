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

import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;
import org.chodavarapu.jgitaws.JGitAwsConfiguration;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectIdRef;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.SymbolicRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.util.function.Supplier;

/**
 * @author Ravi Chodavarapu (rchodava@gmail.com)
 */
public class RefRepository {
    private static final Logger logger = LoggerFactory.getLogger(RefRepository.class);

    private static final String IS_PEELED_ATTRIBUTE = "IsPeeled";
    private static final String IS_SYMBOLIC_ATTRIBUTE = "IsSymbolic";
    private static final String NAME_ATTRIBUTE = "Name";
    private static final String PEELED_TARGET_ATTRIBUTE = "PeeledTarget";
    private static final String REPOSITORY_NAME_ATTRIBUTE = "RepositoryName";
    private static final String TARGET_ATTRIBUTE = "Target";
    private static final String COMPARE_AND_PUT_EXPRESSION = "SET " +
            TARGET_ATTRIBUTE + " = :target, " +
            IS_PEELED_ATTRIBUTE + " = :isPeeled, " +
            IS_SYMBOLIC_ATTRIBUTE + " = :isSymbolic";

    private final JGitAwsConfiguration configuration;
    private final Supplier<CreateTableRequest> tableCreator;

    public RefRepository(JGitAwsConfiguration configuration) {
        this.configuration = configuration;
        this.tableCreator = () ->
                new CreateTableRequest()
                        .withTableName(configuration.getRefsTableName())
                        .withKeySchema(
                                new KeySchemaElement()
                                        .withAttributeName(REPOSITORY_NAME_ATTRIBUTE)
                                        .withKeyType(KeyType.HASH),
                                new KeySchemaElement()
                                        .withAttributeName(NAME_ATTRIBUTE)
                                        .withKeyType(KeyType.RANGE))
                        .withAttributeDefinitions(
                                new AttributeDefinition()
                                        .withAttributeName(REPOSITORY_NAME_ATTRIBUTE)
                                        .withAttributeType(ScalarAttributeType.S),
                                new AttributeDefinition()
                                        .withAttributeName(NAME_ATTRIBUTE)
                                        .withAttributeType(ScalarAttributeType.S))
                        .withProvisionedThroughput(new ProvisionedThroughput(
                                configuration.getInitialRefsTableReadThroughput(),
                                configuration.getInitialRefsTableWriteThroughput()));
    }

    public Observable<Boolean> compareAndPut(String repositoryName, Ref oldRef, Ref newRef) {
        boolean isSymbolic = newRef.isSymbolic();
        boolean isPeeled = newRef.isPeeled();
        String target = newRef.isSymbolic() ? newRef.getTarget().getName() : newRef.getObjectId().name();

        logger.debug("Saving ref {} -> {} in repository {}", newRef.getName(), target, repositoryName);

        UpdateItemSpec updateSpec = new UpdateItemSpec()
                .withPrimaryKey(new PrimaryKey(
                        new KeyAttribute(REPOSITORY_NAME_ATTRIBUTE, repositoryName),
                        new KeyAttribute(NAME_ATTRIBUTE, newRef.getName())));

        StringBuilder updateExpression = new StringBuilder(COMPARE_AND_PUT_EXPRESSION);
        ValueMap valueMap = new ValueMap()
                .withString(":target", target)
                .withBoolean(":isSymbolic", isSymbolic)
                .withBoolean(":isPeeled", isPeeled);

        if (isPeeled && newRef.getPeeledObjectId() != null) {
            updateExpression.append(", ");
            updateExpression.append(PEELED_TARGET_ATTRIBUTE);
            updateExpression.append(" = :peeledTarget");
            valueMap = valueMap.withString(":peeledTarget", newRef.getPeeledObjectId().name());
        }

        if (oldRef != null && oldRef.getStorage() != Ref.Storage.NEW) {
            String expected = oldRef.isSymbolic() ? oldRef.getTarget().getName() : oldRef.getObjectId().name();
            updateSpec = updateSpec.withConditionExpression("#target = :expected")
                    .withNameMap(new NameMap().with("#target", TARGET_ATTRIBUTE));
            valueMap = valueMap.withString(":expected", expected);
        }

        updateSpec = updateSpec.withUpdateExpression(updateExpression.toString()).withValueMap(valueMap);

        return configuration.getDynamoClient().updateItem(configuration.getRefsTableName(), updateSpec, tableCreator)
                .map(v -> true)
                .doOnNext(v -> logger.debug("Saved ref {} in repository {}", newRef.getName(), repositoryName))
                .onErrorReturn(t -> false);
    }

    public Observable<Boolean> compareAndRemove(String repositoryName, Ref ref) {
        String expected = ref.isSymbolic() ? ref.getTarget().getName() : ref.getObjectId().name();
        logger.debug("Removing ref {} -> {} from repository {}", ref.getName(), expected, repositoryName);

        return configuration.getDynamoClient().deleteItem(
                configuration.getRefsTableName(),
                new DeleteItemSpec()
                        .withPrimaryKey(new PrimaryKey(
                                new KeyAttribute(REPOSITORY_NAME_ATTRIBUTE, repositoryName),
                                new KeyAttribute(NAME_ATTRIBUTE, ref.getName())))
                        .withConditionExpression("#target = :expected")
                        .withNameMap(new NameMap()
                                .with("#target", TARGET_ATTRIBUTE))
                        .withValueMap(new ValueMap()
                                .with(":expected", expected)))
                .map(v -> true)
                .doOnNext(v -> logger.debug("Removed ref {} -> {} from repository {}", ref.getName(), expected, repositoryName))
                .onErrorReturn(t -> false);
    }

    public Observable<Ref> getAllRefsSorted(String repositoryName) {
        return configuration.getDynamoClient().getAllItems(
                configuration.getRefsTableName(),
                new QuerySpec()
                        .withHashKey(REPOSITORY_NAME_ATTRIBUTE, repositoryName)
                        .withScanIndexForward(true))
                .map(item -> {
                    String name = item.getString(NAME_ATTRIBUTE);
                    String target = item.getString(TARGET_ATTRIBUTE);
                    boolean isSymbolic = item.getBoolean(IS_SYMBOLIC_ATTRIBUTE);
                    boolean isPeeled = item.getBoolean(IS_PEELED_ATTRIBUTE);
                    String peeledTarget = item.getString(PEELED_TARGET_ATTRIBUTE);

                    if (isSymbolic) {
                        return new SymbolicRef(name, new ObjectIdRef.Unpeeled(Ref.Storage.PACKED, target, null));
                    } else {
                        if (isPeeled) {
                            if (peeledTarget == null) {
                                return new ObjectIdRef.PeeledNonTag(Ref.Storage.PACKED, name, ObjectId.fromString(target));
                            } else {
                                return new ObjectIdRef.PeeledTag(
                                        Ref.Storage.PACKED,
                                        name,
                                        ObjectId.fromString(target),
                                        ObjectId.fromString(peeledTarget));
                            }
                        } else {
                            return new ObjectIdRef.Unpeeled(Ref.Storage.PACKED, name, ObjectId.fromString(target));
                        }
                    }
                });
    }
}
