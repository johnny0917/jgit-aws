package org.chodavarapu.jgitaws.git;

import org.eclipse.jgit.internal.JGitText;
import org.eclipse.jgit.internal.storage.dfs.*;
import org.eclipse.jgit.lib.*;

import java.io.IOException;
import java.text.MessageFormat;

/**
 * @author Ravi Chodavarapu (rchodava@gmail.com)
 */
public class AmazonRepository extends DfsRepository {
    public static class Builder
            extends DfsRepositoryBuilder {
        private String repositoryPath;

        public String getRepositoryPath() {
            return repositoryPath;
        }

        public Builder setRepositoryPath(String repositoryPath) {
            this.repositoryPath = repositoryPath;
            return this;
        }

        @Override
        public Builder setup() throws IllegalArgumentException, IOException {
            return this;
        }

        @Override
        public AmazonRepository build() throws IOException {
            return new AmazonRepository(setup());
        }
    }

    private final StoredConfig config;
    private final DfsObjDatabase objectDatabase;
    private final DfsRefDatabase refDatabase;

    private AmazonRepository(Builder builder) {
        super(builder);

        config = new DynamoConfig(null, "");
        objectDatabase = new S3ObjectDatabase(this, builder.getReaderOptions());
        refDatabase = new DynamoRefDatabase(this);
    }

    public String getRepositoryPath() {
        return getDescription().getRepositoryName();
    }

    @Override
    public void create(boolean bare) throws IOException {
        if (exists())
            throw new IOException(MessageFormat.format(
                    JGitText.get().repositoryAlreadyExists, "")); //$NON-NLS-1$

        String master = Constants.R_HEADS + Constants.MASTER;
        RefUpdate.Result result = updateRef(Constants.HEAD, true).link(master);
        if (result != RefUpdate.Result.NEW)
            throw new IOException(result.name());
    }

    public boolean exists() throws IOException {
//        return getRefDatabase().exists();
        return true;
    }

    @Override
    public StoredConfig getConfig() {
        return config;
    }

    @Override
    public DfsObjDatabase getObjectDatabase() {
        return objectDatabase;
    }

    @Override
    public DfsRefDatabase getRefDatabase() {
        return refDatabase;
    }

    @Override
    public ReflogReader getReflogReader(String refName) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void notifyIndexChanged() {

    }

    @Override
    public void scanForRepoChanges() throws IOException {

    }

}
