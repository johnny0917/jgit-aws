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

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.chodavarapu.jgitaws.JGitAwsConfiguration;
import org.eclipse.jgit.internal.storage.dfs.ReadableChannel;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * @author Ravi Chodavarapu (rchodava@gmail.com)
 */
public class S3ObjectReadableChannel implements ReadableChannel {
    private final String objectName;
    private JGitAwsConfiguration configuration;
    private boolean open;
    private long position;
    private int readAhead;
    private long size;

    public S3ObjectReadableChannel(JGitAwsConfiguration configuration, String objectName) {
        this.configuration = configuration;
        this.objectName = objectName;
        this.open = true;
        this.position = 0;
        this.readAhead = 0;
        this.size = -1;
    }

    @Override
    public int blockSize() {
        return configuration.getStreamingBlockSize();
    }

    @Override
    public void close() throws IOException {
        open = false;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public long position() throws IOException {
        return position;
    }

    @Override
    public void position(long newPosition) throws IOException {
        this.position = newPosition;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        S3Object object = configuration.getS3Client().getObject(
                new GetObjectRequest(configuration.getPacksBucketName(), objectName)
                        .withRange(position, position + dst.remaining() + readAhead));

        try (InputStream inputStream = object.getObjectContent()) {
            size = object.getObjectMetadata().getInstanceLength();

            int readLength = dst.remaining() + readAhead;
            byte[] buffer = new byte[readLength];

            inputStream.read(buffer);
            dst.put(buffer, 0, readLength);

            return readLength;
        }
    }

    @Override
    public void setReadAheadBytes(int bufferSize) throws IOException {
        this.readAhead = bufferSize;
    }

    @Override
    public long size() throws IOException {
        return size;
    }
}
