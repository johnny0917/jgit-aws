package org.chodavarapu.jgitaws.repositories;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.RequestClientOptions;
import com.amazonaws.services.s3.model.*;
import org.chodavarapu.jgitaws.JGitAwsConfiguration;
import org.eclipse.jgit.internal.storage.dfs.DfsOutputStream;
import org.eclipse.jgit.internal.storage.dfs.DfsPackDescription;
import org.eclipse.jgit.internal.storage.pack.PackExt;
import rx.Observable;
import rx.schedulers.Schedulers;
import rx.util.async.Async;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Ravi Chodavarapu (rchodava@gmail.com)
 */
public class PackRepository {
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

    public DfsOutputStream savePack(String repositoryName, String packName, long length) throws IOException {
        PipedInputStream pipedInputStream = new PipedInputStream(RequestClientOptions.DEFAULT_STREAM_BUFFER_SIZE);
        PipedDfsOutputStream pipedOutputStream = new PipedDfsOutputStream(pipedInputStream, (int) length);

        ObjectMetadata metaData = new ObjectMetadata();
        metaData.setContentLength(length);

        Async.fromAction(() -> {
            String objectName = objectName(repositoryName, packName);
            try {
                configuration.getS3Client().putObject(
                        configuration.getPacksBucketName(), objectName, pipedInputStream, metaData);
            } catch (AmazonServiceException e) {
                if ("InvalidBucketName".equals(e.getErrorCode()) || "InvalidBucketState".equals(e.getErrorCode())) {
                    configuration.getS3Client().createBucket(new CreateBucketRequest(configuration.getPacksBucketName()));
                    configuration.getS3Client().setBucketVersioningConfiguration(
                            new SetBucketVersioningConfigurationRequest(
                                    configuration.getPacksBucketName(),
                                    new BucketVersioningConfiguration(BucketVersioningConfiguration.OFF)));

                    configuration.getS3Client().putObject(
                            configuration.getPacksBucketName(), objectName, pipedInputStream, metaData);
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
