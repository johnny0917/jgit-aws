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
package org.chodavarapu.jgitaws.aws;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import rx.Observable;
import rx.schedulers.Schedulers;
import rx.util.async.Async;

import java.util.function.Supplier;

/**
 * @author Ravi Chodavarapu (rchodava@gmail.com)
 */
public class DynamoClient {
    private final DynamoDB dynamoDb;

    public DynamoClient(DynamoDB dynamoDb) {
        this.dynamoDb = dynamoDb;
    }

    public DynamoClient(AmazonDynamoDB dynamoClient) {
        this.dynamoDb = new DynamoDB(dynamoClient);
    }

    public Observable<Item> getItem(String tableName, PrimaryKey primaryKey) {
        return Async.fromCallable(() -> {
            try {
                return dynamoDb.getTable(tableName).getItem(primaryKey);
            } catch (ResourceNotFoundException e) {
                return null;
            }
        }, Schedulers.io());
    }

    public Observable<Item> getAllItems(String tableName, QuerySpec querySpec) {
        return Async.fromCallable(() -> {
            try {
                return dynamoDb.getTable(tableName).query(querySpec);
            } catch (ResourceNotFoundException e) {
                return null;
            }
        }, Schedulers.io())
                .flatMap(itemCollection -> Observable.from(itemCollection));
    }

    private <T> Observable<Void> update(Supplier<T> updater, Supplier<CreateTableRequest> tableCreator) {
        return Async.fromCallable(() -> {
            try {
                return updater.get();
            } catch (ResourceNotFoundException e) {
                dynamoDb.createTable(tableCreator.get());
                return updater.get();
            }
        }, Schedulers.io())
                .map(o -> null);
    }

    public Observable<Void> updateItem(String tableName, UpdateItemSpec updateItemSpec,
                                       Supplier<CreateTableRequest> tableCreator) {
        return update(() -> dynamoDb.getTable(tableName).updateItem(updateItemSpec), tableCreator);
    }

    public Observable<Void> updateItems(TableWriteItems tableWriteItems,
                                        Supplier<CreateTableRequest> tableCreator) {
        return update(() -> dynamoDb.batchWriteItem(tableWriteItems), tableCreator);
    }
}
