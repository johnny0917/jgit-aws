package org.chodavarapu.jgitaws.repositories;

import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import org.chodavarapu.jgitaws.JGitAwsConfiguration;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectIdRef;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.SymbolicRef;
import rx.Observable;

/**
 * @author Ravi Chodavarapu (rchodava@gmail.com)
 */
public class RefRepository {
    private static final String IS_PEELED_ATTRIBUTE = "IsPeeled";
    private static final String IS_SYMBOLIC_ATTRIBUTE = "IsSymbolic";
    private static final String NAME_ATTRIBUTE = "Name";
    private static final String REPOSITORY_NAME_ATTRIBUTE = "RepositoryName";
    private static final String TARGET_ATTRIBUTE = "Target";

    private final JGitAwsConfiguration configuration;

    public RefRepository(JGitAwsConfiguration configuration) {
        this.configuration = configuration;
    }

    public Observable<Void> addRefIfAbsent(String repositoryName, Ref ref) {
        String name = ref.getName();
        String target = ref.getTarget().getObjectId().name();
        boolean isSymbolic = ref.isSymbolic();
        boolean isPeeled = ref.isPeeled();
        return Observable.just(null);
    }

    public Observable<Ref> getAllRefsSorted(String repositoryName) {
        return configuration.getDynamoClient().getAllItems(
                configuration.getRefsTableName(),
                new QuerySpec()
                        .withHashKey(REPOSITORY_NAME_ATTRIBUTE, repositoryName)
                        .withScanIndexForward(true))
                .map(item -> {
                    String name = item.getString(NAME_ATTRIBUTE);
                    String target = item.getString(TARGET_ATTRIBUTE);
                    boolean isSymbolic = item.getBoolean(IS_SYMBOLIC_ATTRIBUTE);

                    if (isSymbolic) {
                        return new SymbolicRef(name, new ObjectIdRef.Unpeeled(Ref.Storage.PACKED, target, null));
                    } else {
                        return new ObjectIdRef.Unpeeled(Ref.Storage.PACKED, name, ObjectId.fromString(target));
                    }
                });
    }
}
