package org.chodavarapu.jgitaws.repositories;

import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.*;
import org.chodavarapu.jgitaws.JGitAwsConfiguration;
import rx.Observable;

/**
 * @author Ravi Chodavarapu (rchodava@gmail.com)
 */
public class ConfigurationRepository {
    private static final String REPOSITORY_NAME_ATTRIBUTE = "RepositoryName";
    private static final String TEXT_ATTRIBUTE = "Text";

    private final JGitAwsConfiguration configuration;

    public ConfigurationRepository(JGitAwsConfiguration configuration) {
        this.configuration = configuration;
    }

    public Observable<String> getConfiguration(String repositoryName) {
        return configuration.getDynamoClient().getItem(
                configuration.getConfigurationsTableName(),
                new PrimaryKey(
                        REPOSITORY_NAME_ATTRIBUTE,
                        repositoryName))
                .map(item -> item == null ? null : item.getString(TEXT_ATTRIBUTE));
    }

    public Observable<Void> updateConfiguration(String repositoryName, String text) {
        return configuration.getDynamoClient().updateItem(configuration.getConfigurationsTableName(),
                new UpdateItemSpec()
                        .withPrimaryKey(REPOSITORY_NAME_ATTRIBUTE, repositoryName)
                        .withAttributeUpdate(new AttributeUpdate(TEXT_ATTRIBUTE).put(text)),
                () -> new CreateTableRequest()
                        .withTableName(configuration.getConfigurationsTableName())
                        .withKeySchema(
                                new KeySchemaElement()
                                        .withAttributeName(REPOSITORY_NAME_ATTRIBUTE)
                                        .withKeyType(KeyType.HASH))
                        .withAttributeDefinitions(
                                new AttributeDefinition()
                                        .withAttributeName(REPOSITORY_NAME_ATTRIBUTE)
                                        .withAttributeType(ScalarAttributeType.S),
                                new AttributeDefinition()
                                        .withAttributeName(TEXT_ATTRIBUTE)
                                        .withAttributeType(ScalarAttributeType.S)));
    }
}
