package org.chodavarapu.jgitaws.repositories;

import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.*;
import org.chodavarapu.jgitaws.aws.DynamoClient;
import rx.Observable;

/**
 * @author Ravi Chodavarapu (rchodava@gmail.com)
 */
public class GitConfigurationRepository {
    private static final String gitConfigurationsTableName = "sf.GitConfigurations";

    private final DynamoClient dynamoClient;

    public GitConfigurationRepository(DynamoClient dynamoClient) {
        this.dynamoClient = dynamoClient;
    }

    public Observable<String> getConfiguration(String repositoryPath) {
        return dynamoClient.getItem(gitConfigurationsTableName, new PrimaryKey("Path", repositoryPath))
                .map(item -> item == null ? null : item.getString("Text"));
    }

    public Observable<Void> updateConfiguration(String repositoryPath, String configuration) {
        return dynamoClient.updateItem(gitConfigurationsTableName,
                new UpdateItemSpec()
                        .withPrimaryKey("Path", repositoryPath)
                        .withAttributeUpdate(new AttributeUpdate("Text").put(configuration)),
                () -> new CreateTableRequest()
                        .withTableName(gitConfigurationsTableName)
                        .withKeySchema(new KeySchemaElement()
                                .withAttributeName("Path")
                                .withKeyType(KeyType.HASH))
                        .withAttributeDefinitions(
                                new AttributeDefinition()
                                        .withAttributeName("Id")
                                        .withAttributeType(ScalarAttributeType.S),
                                new AttributeDefinition()
                                        .withAttributeName("Text")
                                        .withAttributeType(ScalarAttributeType.S)));
    }
}
