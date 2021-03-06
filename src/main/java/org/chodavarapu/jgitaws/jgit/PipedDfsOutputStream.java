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

import org.eclipse.jgit.internal.storage.dfs.DfsOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;

/**
 * @author Ravi Chodavarapu (rchodava@gmail.com)
 */
public class PipedDfsOutputStream extends DfsOutputStream {
    private static final Logger logger = LoggerFactory.getLogger(PipedDfsOutputStream.class);
    private final PipedOutputStream out;

    // TODO: Doing this with a second buffer is not a good approach. Come up with better one.
    private final GrowingByteBuffer readBackSupportBuffer;

    private final String objectName;
    private final int blockSize;

    public PipedDfsOutputStream(PipedInputStream pipedInputStream, String objectName, int totalLength, int blockSize) throws IOException {
        this.out = new PipedOutputStream(pipedInputStream);
        this.objectName = objectName;
        this.readBackSupportBuffer = new GrowingByteBuffer(totalLength < 1 ? blockSize * 4 : totalLength);
        this.blockSize = blockSize;
    }

    @Override
    public int blockSize() {
        return blockSize;
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);

        synchronized (readBackSupportBuffer) {
            readBackSupportBuffer.write(b);
        }
    }

    @Override
    public void write(byte[] b) throws IOException {
        out.write(b);

        synchronized (readBackSupportBuffer) {
            readBackSupportBuffer.write(b);
        }
    }

    @Override
    public void write(byte[] buf, int off, int len) throws IOException {
        out.write(buf, off, len);

        synchronized (readBackSupportBuffer) {
            readBackSupportBuffer.write(buf, off, len);
        }
    }

    @Override
    public int read(long position, ByteBuffer buf) throws IOException {
        synchronized (readBackSupportBuffer) {
            return readBackSupportBuffer.read(position, buf);
        }
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void close() throws IOException {
        logger.debug("Finished writing file {} to S3 bucket", objectName);
        out.close();
    }

    private static class GrowingByteBuffer extends ByteArrayOutputStream {
        public GrowingByteBuffer(int initialCapacity) {
            super(initialCapacity);
        }

        public int read(long position, ByteBuffer dest) {
            int numberOfBytesToRead = dest.remaining();

            numberOfBytesToRead = (int) Math.min(numberOfBytesToRead, count - position);
            dest.put(buf, (int) position, numberOfBytesToRead);

            return numberOfBytesToRead;
        }
    }
}
