package org.chodavarapu.jgitaws.aws;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
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

    public DynamoClient(AmazonDynamoDB dynamoClient) {
        this.dynamoDb = new DynamoDB(dynamoClient);
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

    public Observable<Item> getAllItems(String tableName, QuerySpec querySpec) {
        return Async.fromCallable(() -> {
            try {
                return dynamoDb.getTable(tableName).query(querySpec);
            } catch (ResourceNotFoundException e) {
                return null;
            }
        }, Schedulers.io())
                .flatMap(itemCollection -> Observable.from(itemCollection));
    }

    private <T> Observable<Void> update(Supplier<T> updater, Supplier<CreateTableRequest> tableCreator) {
        return Async.fromCallable(() -> {
            try {
                return updater.get();
            } catch (ResourceNotFoundException e) {
                dynamoDb.createTable(tableCreator.get());
                return updater.get();
            }
        }, Schedulers.io())
                .map(o -> null);
    }

    public Observable<Void> updateItem(String tableName, UpdateItemSpec updateItemSpec,
                                       Supplier<CreateTableRequest> tableCreator) {
        return update(() -> dynamoDb.getTable(tableName).updateItem(updateItemSpec), tableCreator);
    }

    public Observable<Void> updateItems(TableWriteItems tableWriteItems,
                                        Supplier<CreateTableRequest> tableCreator) {
        return update(() -> dynamoDb.batchWriteItem(tableWriteItems), tableCreator);
    }
}
