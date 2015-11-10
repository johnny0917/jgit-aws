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

import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import org.chodavarapu.jgitaws.JGitAwsConfiguration;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectIdRef;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.SymbolicRef;
import rx.Observable;

/**
 * @author Ravi Chodavarapu (rchodava@gmail.com)
 */
public class RefRepository {
    private static final String IS_PEELED_ATTRIBUTE = "IsPeeled";
    private static final String IS_SYMBOLIC_ATTRIBUTE = "IsSymbolic";
    private static final String NAME_ATTRIBUTE = "Name";
    private static final String REPOSITORY_NAME_ATTRIBUTE = "RepositoryName";
    private static final String TARGET_ATTRIBUTE = "Target";

    private final JGitAwsConfiguration configuration;

    public RefRepository(JGitAwsConfiguration configuration) {
        this.configuration = configuration;
    }

    public Observable<Void> addRefIfAbsent(String repositoryName, Ref ref) {
        String name = ref.getName();
        String target = ref.getTarget().getObjectId().name();
        boolean isSymbolic = ref.isSymbolic();
        boolean isPeeled = ref.isPeeled();
        return Observable.just(null);
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

                    if (isSymbolic) {
                        return new SymbolicRef(name, new ObjectIdRef.Unpeeled(Ref.Storage.PACKED, target, null));
                    } else {
                        return new ObjectIdRef.Unpeeled(Ref.Storage.PACKED, name, ObjectId.fromString(target));
                    }
                });
    }
}
