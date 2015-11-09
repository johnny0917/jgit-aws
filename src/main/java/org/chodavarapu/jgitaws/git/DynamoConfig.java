package org.chodavarapu.jgitaws.git;

import org.chodavarapu.jgitaws.repositories.GitConfigurationRepository;
import org.eclipse.jgit.errors.ConfigInvalidException;
import org.eclipse.jgit.lib.StoredConfig;

import java.io.IOException;

/**
 * @author Ravi Chodavarapu (rchodava@gmail.com)
 */
public class DynamoConfig extends StoredConfig {
    private static final int MAX_ITEM_SIZE = 399 * 1024;

    private final GitConfigurationRepository gitConfigurationRepository;
    private final String repositoryPath;

    public DynamoConfig(GitConfigurationRepository gitConfigurationRepository, String repositoryPath) {
        this.gitConfigurationRepository = gitConfigurationRepository;
        this.repositoryPath = repositoryPath;
    }

    @Override
    public void load() throws IOException, ConfigInvalidException {
        String configuration = gitConfigurationRepository.getConfiguration(repositoryPath)
                .toBlocking().last();
        fromText(configuration);
    }

    @Override
    public void save() throws IOException {
        String configuration = toText();
        if (configuration.length() > MAX_ITEM_SIZE) {
            throw new IOException(new IllegalArgumentException("Configuration is too large!"));
        }

        gitConfigurationRepository.updateConfiguration(repositoryPath, configuration)
                .toBlocking().last();
    }
}
