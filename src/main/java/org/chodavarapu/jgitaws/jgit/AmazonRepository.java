package org.chodavarapu.jgitaws.jgit;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import org.chodavarapu.jgitaws.JGitAwsConfiguration;
import org.chodavarapu.jgitaws.aws.DynamoClient;
import org.eclipse.jgit.internal.JGitText;
import org.eclipse.jgit.internal.storage.dfs.DfsObjDatabase;
import org.eclipse.jgit.internal.storage.dfs.DfsRefDatabase;
import org.eclipse.jgit.internal.storage.dfs.DfsRepository;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryBuilder;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.RefUpdate;
import org.eclipse.jgit.lib.ReflogReader;
import org.eclipse.jgit.lib.StoredConfig;

import java.io.IOException;
import java.text.MessageFormat;

/**
 * @author Ravi Chodavarapu (rchodava@gmail.com)
 */
public class AmazonRepository extends DfsRepository {
    private final StoredConfig config;
    private final DfsObjDatabase objectDatabase;
    private final DfsRefDatabase refDatabase;
    private AmazonRepository(Builder builder) {
        super(builder);

        DynamoClient client = new DynamoClient((AmazonDynamoDB) null);
        config = new DynamoStoredConfig(null, getRepositoryName());
        objectDatabase =
                new S3WithDynamoMetaDataObjDatabase(
                        this,
                        builder.getReaderOptions(),
                        new JGitAwsConfiguration(client, null));
        refDatabase = new DynamoRefDatabase(this);
    }

    public String getRepositoryName() {
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

    public static class Builder
            extends DfsRepositoryBuilder {
        @Override
        public Builder setup() throws IllegalArgumentException, IOException {
            return this;
        }

        @Override
        public AmazonRepository build() throws IOException {
            return new AmazonRepository(setup());
        }
    }

}
