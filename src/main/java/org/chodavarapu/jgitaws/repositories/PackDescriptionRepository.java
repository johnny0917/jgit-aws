package org.chodavarapu.jgitaws.repositories;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.*;
import org.chodavarapu.jgitaws.JGitAwsConfiguration;
import org.chodavarapu.jgitaws.jgit.AmazonRepository;
import org.eclipse.jgit.internal.storage.dfs.DfsObjDatabase;
import org.eclipse.jgit.internal.storage.dfs.DfsPackDescription;
import org.eclipse.jgit.internal.storage.pack.PackExt;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * @author Ravi Chodavarapu (rchodava@gmail.com)
 */
public class PackDescriptionRepository {
    private static final Logger logger = LoggerFactory.getLogger(PackDescriptionRepository.class);

    private static final int MAXIMUM_OPERATIONS_PER_BATCH = 25;

    private static final String NAME_ATTRIBUTE = "Name";
    private static final String REPOSITORY_NAME_ATTRIBUTE = "RepositoryName";
    private static final String DESCRIPTION_ATTRIBUTE = "Description";

    private final JGitAwsConfiguration configuration;
    private final Supplier<CreateTableRequest> tableCreator;

    public PackDescriptionRepository(JGitAwsConfiguration configuration) {
        this.configuration = configuration;
        this.tableCreator = () ->
                new CreateTableRequest()
                        .withTableName(configuration.getConfigurationsTableName())
                        .withKeySchema(
                                new KeySchemaElement()
                                        .withAttributeName(REPOSITORY_NAME_ATTRIBUTE)
                                        .withKeyType(KeyType.HASH),
                                new KeySchemaElement()
                                        .withAttributeName(NAME_ATTRIBUTE)
                                        .withKeyType(KeyType.RANGE))
                        .withAttributeDefinitions(
                                new AttributeDefinition()
                                        .withAttributeName(REPOSITORY_NAME_ATTRIBUTE)
                                        .withAttributeType(ScalarAttributeType.S),
                                new AttributeDefinition()
                                        .withAttributeName(NAME_ATTRIBUTE)
                                        .withAttributeType(ScalarAttributeType.S),
                                new AttributeDefinition()
                                        .withAttributeName(DESCRIPTION_ATTRIBUTE)
                                        .withAttributeType(ScalarAttributeType.S));
    }

    private static DfsPackDescription fromJson(String json, DfsPackDescription desc) {
        JSONObject object = new JSONObject(json);

        desc.setPackSource(object.optEnum(DfsObjDatabase.PackSource.class, "source"))
                .setLastModified(object.optLong("modified"))
                .setObjectCount(object.optLong("objects"))
                .setDeltaCount(object.optLong("deltas"))
                .setIndexVersion(object.optInt("ixVersion"))
                .clearPackStats();

        for (PackExt ext : PackExt.values()) {
            if (object.has(ext.getExtension())) {
                desc.addFileExt(ext);
                desc.setFileSize(ext, object.getLong(ext.getExtension()));
            }
        }

        return desc;
    }

    private static String toJson(DfsPackDescription desc) {
        JSONObject json = new JSONObject()
                .put("source", desc.getPackSource().name())
                .put("modified", desc.getLastModified())
                .put("objects", desc.getObjectCount())
                .put("deltas", desc.getDeltaCount())
                .put("ixVersion", desc.getIndexVersion());

        for (PackExt ext : PackExt.values()) {
            if (desc.hasFileExt(ext)) {
                json.put(ext.getExtension() + "Size", desc.getFileSize(ext));
            }
        }

        return json.toString();
    }

    private String packName(DfsPackDescription desc) {
        String packName = desc.getFileName(PackExt.PACK);
        int packNameSeparatorIx = packName.indexOf('.');
        if (packNameSeparatorIx > 0) {
            packName = packName.substring(0, packNameSeparatorIx);
        }

        return packName;
    }

    private java.util.stream.Stream<DfsPackDescription> operationsOfType(
            Collection<PackDescriptionOperation> operations,
            Operation type) {
        return operations.stream()
                .filter(operation -> operation.getOperation() == type)
                .map(operation -> operation.getDescription());
    }

    public Observable<Void> updatePackDescriptions(
            Collection<DfsPackDescription> additions,
            Collection<DfsPackDescription> removals) {

        return (additions == null ? Observable.<DfsPackDescription>empty() : Observable.from(additions))
                .map(addition -> new PackDescriptionOperation(addition, Operation.ADDITION))
                .concatWith(
                        (removals == null ? Observable.<DfsPackDescription>empty() : Observable.from(removals))
                                .map(removal -> new PackDescriptionOperation(removal, Operation.REMOVAL)))
                .window(MAXIMUM_OPERATIONS_PER_BATCH)
                .flatMap(portions -> portions.toList())
                .map(portion -> createBatchRequest(portion))
                .flatMap(request -> configuration.getDynamoClient().updateItems(request, tableCreator))
                .doOnCompleted(() -> logger.debug("Commit of packs to S3 complete!"));
    }

    private TableWriteItems createBatchRequest(List<PackDescriptionOperation> operations) {
        List<Item> itemsToPut = createItemsToPutList(operations);
        List<PrimaryKey> keysToDelete = createKeysToDeleteList(operations);

        TableWriteItems request = new TableWriteItems(configuration.getPackDescriptionsTableName());

        if (itemsToPut.size() > 0) {
            request.withItemsToPut(itemsToPut);
        }

        if (keysToDelete.size() > 0) {
            request.withPrimaryKeysToDelete(keysToDelete.toArray(new PrimaryKey[keysToDelete.size()]));
        }

        return request;
    }

    private List<PrimaryKey> createKeysToDeleteList(List<PackDescriptionOperation> portion) {
        return operationsOfType(portion, Operation.REMOVAL)
                .map(removal ->
                        new PrimaryKey(
                                new KeyAttribute(REPOSITORY_NAME_ATTRIBUTE,
                                        removal.getRepositoryDescription().getRepositoryName()),
                                new KeyAttribute(NAME_ATTRIBUTE, packName(removal))))
                .collect(Collectors.toList());
    }

    private List<Item> createItemsToPutList(List<PackDescriptionOperation> portion) {
        return operationsOfType(portion, Operation.ADDITION)
                .map(addition ->
                        new Item()
                                .withPrimaryKey(new PrimaryKey(
                                        new KeyAttribute(REPOSITORY_NAME_ATTRIBUTE,
                                                addition.getRepositoryDescription().getRepositoryName()),
                                        new KeyAttribute(NAME_ATTRIBUTE, packName(addition))))
                                .withString(DESCRIPTION_ATTRIBUTE, toJson(addition)))
                .collect(Collectors.toList());
    }

    public Observable<DfsPackDescription> getAllPackDescriptions(AmazonRepository repository) {
        return configuration.getDynamoClient().getAllItems(
                configuration.getPackDescriptionsTableName(),
                new QuerySpec()
                        .withHashKey(REPOSITORY_NAME_ATTRIBUTE, repository.getRepositoryName())
                        .withScanIndexForward(true))
                .map(item -> {
                    String name = item.getString(NAME_ATTRIBUTE);
                    String description = item.getString(DESCRIPTION_ATTRIBUTE);

                    return fromJson(description, new DfsPackDescription(repository.getDescription(), name));
                })
                .doOnCompleted(() ->
                        logger.debug("Retrieved packs list for repository {}", repository.getRepositoryName()));
    }

    private enum Operation {
        ADDITION,
        REMOVAL
    }

    private static class PackDescriptionOperation {
        Operation operation;
        DfsPackDescription description;

        public PackDescriptionOperation(DfsPackDescription description, Operation operation) {
            this.description = description;
            this.operation = operation;
        }

        public Operation getOperation() {
            return operation;
        }

        public DfsPackDescription getDescription() {
            return description;
        }
    }
}
