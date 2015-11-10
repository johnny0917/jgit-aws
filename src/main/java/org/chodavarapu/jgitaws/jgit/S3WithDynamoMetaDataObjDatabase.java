package org.chodavarapu.jgitaws.jgit;

import org.chodavarapu.jgitaws.JGitAwsConfiguration;
import org.eclipse.jgit.internal.storage.dfs.*;
import org.eclipse.jgit.internal.storage.pack.PackExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author Ravi Chodavarapu (rchodava@gmail.com)
 */
public class S3WithDynamoMetaDataObjDatabase extends DfsObjDatabase {
    private static final Logger logger = LoggerFactory.getLogger(S3WithDynamoMetaDataObjDatabase.class);

    private final JGitAwsConfiguration configuration;
    private final SecureRandom random = new SecureRandom();

    public S3WithDynamoMetaDataObjDatabase(
            AmazonRepository repository,
            DfsReaderOptions readerOptions,
            JGitAwsConfiguration configuration) {
        super(repository, readerOptions);

        this.configuration = configuration;
    }

    @Override
    protected void commitPackImpl(Collection<DfsPackDescription> desc, Collection<DfsPackDescription> replaces) throws IOException {
        logger.debug("Committing packs for repository {}", getRepository().getDescription().getRepositoryName());
        configuration.getPackDescriptionRepository().updatePackDescriptions(desc, replaces).toBlocking().last();
    }

    @Override
    protected List<DfsPackDescription> listPacks() throws IOException {
        logger.debug("Retrieving list of packs for repository {}", getRepository().getDescription().getRepositoryName());
        return configuration.getPackDescriptionRepository().getAllPackDescriptions((AmazonRepository) getRepository())
                .toList()
                .toBlocking()
                .lastOrDefault(Collections.emptyList());
    }

    @Override
    protected ReadableChannel openFile(DfsPackDescription desc, PackExt ext) throws IOException {
        return null;
    }

    @Override
    protected DfsPackDescription newPack(PackSource source) throws IOException {
        StringBuilder packName = new StringBuilder();
        switch (source) {
            case COMPACT: packName.append("cmp-"); break;
            case GC: packName.append("gc-"); break;
            case INSERT: packName.append("ins-"); break;
            case RECEIVE: packName.append("rec-"); break;
            case UNREACHABLE_GARBAGE: packName.append("ug-"); break;
        }
        packName.append(System.currentTimeMillis());
        packName.append('-');
        packName.append(random.nextInt(100));

        logger.debug("Created new pack file {}", packName);
        return new DfsPackDescription(getRepository().getDescription(), packName.toString());
    }

    @Override
    protected void rollbackPack(Collection<DfsPackDescription> desc) {
        try {
            configuration.getPackRepository().deletePacks(desc).toBlocking().last();
        } catch (Exception e) {
            logger.debug("Error occurred while trying to rollback a pack commit operation!", e);
        }
    }

    @Override
    protected DfsOutputStream writeFile(DfsPackDescription desc, PackExt ext) throws IOException {
        logger.debug("Writing pack file {} to S3 bucket", desc.getFileName(ext));
        return configuration.getPackRepository().savePack(
                desc.getRepositoryDescription().getRepositoryName(),
                desc.getFileName(ext),
                desc.getFileSize(ext));
    }
}
