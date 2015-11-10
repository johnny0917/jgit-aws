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

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.*;
import org.chodavarapu.jgitaws.JGitAwsConfiguration;
import org.chodavarapu.jgitaws.jgit.AmazonRepository;
import org.eclipse.jgit.internal.storage.dfs.DfsObjDatabase;
import org.eclipse.jgit.internal.storage.dfs.DfsPackDescription;
import org.eclipse.jgit.internal.storage.pack.PackExt;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * @author Ravi Chodavarapu (rchodava@gmail.com)
 */
public class PackDescriptionRepository {
    private static final Logger logger = LoggerFactory.getLogger(PackDescriptionRepository.class);

    private static final int MAXIMUM_OPERATIONS_PER_BATCH = 25;

    private static final String NAME_ATTRIBUTE = "Name";
    private static final String REPOSITORY_NAME_ATTRIBUTE = "RepositoryName";
    private static final String DESCRIPTION_ATTRIBUTE = "Description";

    private final JGitAwsConfiguration configuration;
    private final Supplier<CreateTableRequest> tableCreator;

    public PackDescriptionRepository(JGitAwsConfiguration configuration) {
        this.configuration = configuration;
        this.tableCreator = () ->
                new CreateTableRequest()
                        .withTableName(configuration.getConfigurationsTableName())
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
                                        .withAttributeType(ScalarAttributeType.S),
                                new AttributeDefinition()
                                        .withAttributeName(DESCRIPTION_ATTRIBUTE)
                                        .withAttributeType(ScalarAttributeType.S));
    }

    private static DfsPackDescription fromJson(String json, DfsPackDescription desc) {
        JSONObject object = new JSONObject(json);

        desc.setPackSource(object.optEnum(DfsObjDatabase.PackSource.class, "source"))
                .setLastModified(object.optLong("modified"))
                .setObjectCount(object.optLong("objects"))
                .setDeltaCount(object.optLong("deltas"))
                .setIndexVersion(object.optInt("ixVersion"))
                .clearPackStats();

        for (PackExt ext : PackExt.values()) {
            if (object.has(ext.getExtension())) {
                desc.addFileExt(ext);
                desc.setFileSize(ext, object.getLong(ext.getExtension()));
            }
        }

        return desc;
    }

    private static String toJson(DfsPackDescription desc) {
        JSONObject json = new JSONObject()
                .put("source", desc.getPackSource().name())
                .put("modified", desc.getLastModified())
                .put("objects", desc.getObjectCount())
                .put("deltas", desc.getDeltaCount())
                .put("ixVersion", desc.getIndexVersion());

        for (PackExt ext : PackExt.values()) {
            if (desc.hasFileExt(ext)) {
                json.put(ext.getExtension() + "Size", desc.getFileSize(ext));
            }
        }

        return json.toString();
    }

    private String packName(DfsPackDescription desc) {
        String packName = desc.getFileName(PackExt.PACK);
        int packNameSeparatorIx = packName.indexOf('.');
        if (packNameSeparatorIx > 0) {
            packName = packName.substring(0, packNameSeparatorIx);
        }

        return packName;
    }

    private java.util.stream.Stream<DfsPackDescription> operationsOfType(
            Collection<PackDescriptionOperation> operations,
            Operation type) {
        return operations.stream()
                .filter(operation -> operation.getOperation() == type)
                .map(operation -> operation.getDescription());
    }

    public Observable<Void> updatePackDescriptions(
            Collection<DfsPackDescription> additions,
            Collection<DfsPackDescription> removals) {

        return (additions == null ? Observable.<DfsPackDescription>empty() : Observable.from(additions))
                .map(addition -> new PackDescriptionOperation(addition, Operation.ADDITION))
                .concatWith(
                        (removals == null ? Observable.<DfsPackDescription>empty() : Observable.from(removals))
                                .map(removal -> new PackDescriptionOperation(removal, Operation.REMOVAL)))
                .window(MAXIMUM_OPERATIONS_PER_BATCH)
                .flatMap(portions -> portions.toList())
                .map(portion -> createBatchRequest(portion))
                .flatMap(request -> configuration.getDynamoClient().updateItems(request, tableCreator))
                .doOnCompleted(() -> logger.debug("Commit of packs to S3 complete!"));
    }

    private TableWriteItems createBatchRequest(List<PackDescriptionOperation> operations) {
        List<Item> itemsToPut = createItemsToPutList(operations);
        List<PrimaryKey> keysToDelete = createKeysToDeleteList(operations);

        TableWriteItems request = new TableWriteItems(configuration.getPackDescriptionsTableName());

        if (itemsToPut.size() > 0) {
            request.withItemsToPut(itemsToPut);
        }

        if (keysToDelete.size() > 0) {
            request.withPrimaryKeysToDelete(keysToDelete.toArray(new PrimaryKey[keysToDelete.size()]));
        }

        return request;
    }

    private List<PrimaryKey> createKeysToDeleteList(List<PackDescriptionOperation> portion) {
        return operationsOfType(portion, Operation.REMOVAL)
                .map(removal ->
                        new PrimaryKey(
                                new KeyAttribute(REPOSITORY_NAME_ATTRIBUTE,
                                        removal.getRepositoryDescription().getRepositoryName()),
                                new KeyAttribute(NAME_ATTRIBUTE, packName(removal))))
                .collect(Collectors.toList());
    }

    private List<Item> createItemsToPutList(List<PackDescriptionOperation> portion) {
        return operationsOfType(portion, Operation.ADDITION)
                .map(addition ->
                        new Item()
                                .withPrimaryKey(new PrimaryKey(
                                        new KeyAttribute(REPOSITORY_NAME_ATTRIBUTE,
                                                addition.getRepositoryDescription().getRepositoryName()),
                                        new KeyAttribute(NAME_ATTRIBUTE, packName(addition))))
                                .withString(DESCRIPTION_ATTRIBUTE, toJson(addition)))
                .collect(Collectors.toList());
    }

    public Observable<DfsPackDescription> getAllPackDescriptions(AmazonRepository repository) {
        return configuration.getDynamoClient().getAllItems(
                configuration.getPackDescriptionsTableName(),
                new QuerySpec()
                        .withHashKey(REPOSITORY_NAME_ATTRIBUTE, repository.getRepositoryName())
                        .withScanIndexForward(true))
                .map(item -> {
                    String name = item.getString(NAME_ATTRIBUTE);
                    String description = item.getString(DESCRIPTION_ATTRIBUTE);

                    return fromJson(description, new DfsPackDescription(repository.getDescription(), name));
                })
                .doOnCompleted(() ->
                        logger.debug("Retrieved packs list for repository {}", repository.getRepositoryName()));
    }

    private enum Operation {
        ADDITION,
        REMOVAL
    }

    private static class PackDescriptionOperation {
        Operation operation;
        DfsPackDescription description;

        public PackDescriptionOperation(DfsPackDescription description, Operation operation) {
            this.description = description;
            this.operation = operation;
        }

        public Operation getOperation() {
            return operation;
        }

        public DfsPackDescription getDescription() {
            return description;
        }
    }
}
