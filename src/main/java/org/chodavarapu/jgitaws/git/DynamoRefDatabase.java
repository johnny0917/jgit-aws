package org.chodavarapu.jgitaws.git;

import org.chodavarapu.jgitaws.repositories.GitRefRepository;
import org.eclipse.jgit.internal.storage.dfs.DfsRefDatabase;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.revwalk.RevWalk;

import java.io.IOException;

/**
 * @author Ravi Chodavarapu (rchodava@gmail.com)
 */
public class DynamoRefDatabase extends DfsRefDatabase {
    private GitRefRepository refRepository;

    public DynamoRefDatabase(AmazonRepository repository) {
        super(repository);
    }

    @Override
    protected boolean compareAndPut(Ref oldRef, Ref newRef) throws IOException {
        ObjectId id = newRef.getObjectId();
        if (id != null) {
            try (RevWalk rw = new RevWalk(getRepository())) {
                rw.parseAny(id);
            }
        }

        String name = newRef.getName();

        if (oldRef == null) {
            refRepository.addRefIfAbsent(
                    getRepository().getRepositoryPath(),
                    newRef.getName(),
                    newRef.getTarget().getObjectId().name(),
                    newRef.isSymbolic());
            return true;
        }

        return false;
    }

    @Override
    protected boolean compareAndRemove(Ref oldRef) throws IOException {
        return false;
    }

    @Override
    protected AmazonRepository getRepository() {
        return (AmazonRepository) super.getRepository();
    }

    @Override
    protected RefCache scanAllRefs() throws IOException {
        return null;
    }
}
