package org.chodavarapu.jgitaws.repositories;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.RequestClientOptions;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;
import org.chodavarapu.jgitaws.JGitAwsConfiguration;
import org.eclipse.jgit.internal.storage.dfs.DfsOutputStream;
import rx.schedulers.Schedulers;
import rx.util.async.Async;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * @author Ravi Chodavarapu (rchodava@gmail.com)
 */
public class PackRepository {
    private static final String PACKS_BUCKET_NAME = "jga.Packs";
    private final JGitAwsConfiguration configuration;

    public PackRepository(JGitAwsConfiguration configuration) {
        this.configuration = configuration;
    }

    public DfsOutputStream savePack(String repositoryName, String packName, long length) throws IOException {
        PipedInputStream pipedInputStream = new PipedInputStream(RequestClientOptions.DEFAULT_STREAM_BUFFER_SIZE);
        PipedDfsOutputStream pipedOutputStream = new PipedDfsOutputStream(pipedInputStream, (int) length);

        ObjectMetadata metaData = new ObjectMetadata();
        metaData.setContentLength(length);

        Async.fromAction(() -> {
            String objectName = new StringBuilder(repositoryName).append('/').append(packName).toString();
            try {
                configuration.getS3Client().putObject(PACKS_BUCKET_NAME, objectName, pipedInputStream, metaData);
            } catch (AmazonServiceException e) {
                if ("InvalidBucketName".equals(e.getErrorCode()) || "InvalidBucketState".equals(e.getErrorCode())) {
                    configuration.getS3Client().createBucket(new CreateBucketRequest(PACKS_BUCKET_NAME));
                    configuration.getS3Client().setBucketVersioningConfiguration(
                            new SetBucketVersioningConfigurationRequest(
                                    PACKS_BUCKET_NAME,
                                    new BucketVersioningConfiguration(BucketVersioningConfiguration.OFF)));

                    configuration.getS3Client().putObject(PACKS_BUCKET_NAME, objectName, pipedInputStream, metaData);
                } else {
                    throw e;
                }
            }
        }, null, Schedulers.io());

        return pipedOutputStream;
    }

    private static class PipedDfsOutputStream extends DfsOutputStream {
        private final PipedOutputStream out;

        // TODO: Doing this with a second buffer is not a good approach. Come up with better one.
        private final byte[] readBackSupportBuffer;

        private int readBackBufferPosition;

        public PipedDfsOutputStream(PipedInputStream pipedInputStream, int totalLength) throws IOException {
            this.out = new PipedOutputStream(pipedInputStream);
            this.readBackSupportBuffer = new byte[totalLength];
            this.readBackBufferPosition = 0;
        }

        @Override
        public int blockSize() {
            return RequestClientOptions.DEFAULT_STREAM_BUFFER_SIZE;
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);

            synchronized (readBackSupportBuffer) {
                readBackSupportBuffer[readBackBufferPosition] = (byte) b;
                readBackBufferPosition++;
            }
        }

        @Override
        public void write(byte[] b) throws IOException {
            out.write(b);

            synchronized (readBackSupportBuffer) {
                System.arraycopy(b, 0, readBackSupportBuffer, readBackBufferPosition, b.length);
                readBackBufferPosition += b.length;
            }
        }

        @Override
        public void write(byte[] buf, int off, int len) throws IOException {
            out.write(buf, off, len);

            synchronized (readBackSupportBuffer) {
                System.arraycopy(buf, off, readBackSupportBuffer, readBackBufferPosition, len);
                readBackBufferPosition += len;
            }
        }

        @Override
        public int read(long position, ByteBuffer buf) throws IOException {
            int numberOfBytesToRead = buf.remaining();

            synchronized (readBackSupportBuffer) {
                numberOfBytesToRead = (int) Math.min(numberOfBytesToRead, readBackBufferPosition - position);
                buf.put(readBackSupportBuffer, (int) position, numberOfBytesToRead);
            }

            return numberOfBytesToRead;
        }

        @Override
        public void flush() throws IOException {
            out.flush();
        }

        @Override
        public void close() throws IOException {
            out.close();
        }
    }
}
