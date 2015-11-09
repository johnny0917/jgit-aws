package org.chodavarapu.jgitaws.aws;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import rx.Observable;
import rx.schedulers.Schedulers;
import rx.util.async.Async;

import java.util.function.Supplier;

/**
 * @author Ravi Chodavarapu (rchodava@gmail.com)
 */
public class DynamoClient {
    private final DynamoDB dynamoDb;

    public DynamoClient(DynamoDB dynamoDb) {
        this.dynamoDb = dynamoDb;
    }

    public Observable<Item> getItem(String tableName, PrimaryKey primaryKey) {
        return Async.fromCallable(() -> {
            try {
                return dynamoDb.getTable(tableName).getItem(primaryKey);
            } catch (ResourceNotFoundException e) {
                return null;
            }
        }, Schedulers.io());
    }

    public Observable<Void> updateItem(String tableName, UpdateItemSpec updateItemSpec,
                                       Supplier<CreateTableRequest> tableCreator) {
        return Async.fromCallable(() -> {
            try {
                return dynamoDb.getTable(tableName).updateItem(updateItemSpec);
            } catch (ResourceNotFoundException e) {
                return dynamoDb.createTable(tableCreator.get()).updateItem(updateItemSpec);
            }
        }, Schedulers.io())
                .map(o -> null);
    }
}
