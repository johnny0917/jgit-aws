package org.chodavarapu.jgitaws.jgit;

import org.chodavarapu.jgitaws.repositories.ConfigurationRepository;
import org.eclipse.jgit.errors.ConfigInvalidException;
import org.eclipse.jgit.lib.StoredConfig;

import java.io.IOException;

/**
 * @author Ravi Chodavarapu (rchodava@gmail.com)
 */
public class DynamoStoredConfig extends StoredConfig {
    private static final int MAX_ITEM_SIZE = 399 * 1024;

    private final ConfigurationRepository configurationRepository;
    private final String repositoryName;

    public DynamoStoredConfig(ConfigurationRepository configurationRepository, String repositoryName) {
        this.configurationRepository = configurationRepository;
        this.repositoryName = repositoryName;
    }

    @Override
    public void load() throws IOException, ConfigInvalidException {
        String configuration = configurationRepository.getConfiguration(repositoryName)
                .toBlocking().last();
        fromText(configuration);
    }

    @Override
    public void save() throws IOException {
        String configuration = toText();
        if (configuration.length() > MAX_ITEM_SIZE) {
            throw new IOException(new IllegalArgumentException("Configuration is too large!"));
        }

        configurationRepository.updateConfiguration(repositoryName, configuration)
                .toBlocking().last();
    }
}
