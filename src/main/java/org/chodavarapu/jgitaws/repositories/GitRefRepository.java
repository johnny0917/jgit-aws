package org.chodavarapu.jgitaws.repositories;

import org.chodavarapu.jgitaws.aws.DynamoClient;
import rx.Observable;

/**
 * @author Ravi Chodavarapu (rchodava@gmail.com)
 */
public class GitRefRepository {
    private static final String gitRefsTableName = "sf.GitRefs";

    private final DynamoClient dynamoClient;

    public GitRefRepository(DynamoClient dynamoClient) {
        this.dynamoClient = dynamoClient;
    }

    public Observable<Void> addRefIfAbsent(String repositoryPath, String name, String target, boolean symbolic) {
        return Observable.just(null);
    }
}
