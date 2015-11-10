package org.chodavarapu.jgitaws.git;

import org.chodavarapu.jgitaws.repositories.RefRepository;
import org.eclipse.jgit.internal.storage.dfs.DfsRefDatabase;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.util.RefList;

import java.io.IOException;

/**
 * @author Ravi Chodavarapu (rchodava@gmail.com)
 */
public class DynamoRefDatabase extends DfsRefDatabase {
    private RefRepository refRepository;

    public DynamoRefDatabase(AmazonRepository repository) {
        super(repository);
    }

    @Override
    protected boolean compareAndPut(Ref oldRef, Ref newRef) throws IOException {
        ObjectId id = newRef.getObjectId();

        // TODO: Is the below RevWalk really necessary? Copying what's done in the InMemoryRepository
        if (id != null) {
            try (RevWalk rw = new RevWalk(getRepository())) {
                rw.parseAny(id);
            }
        }

        String name = newRef.getName();

        if (oldRef == null) {
            refRepository.addRefIfAbsent(
                    getRepository().getRepositoryName(),
                    newRef);
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
        return refRepository.getAllRefsSorted(getRepository().getRepositoryName())
                .toList()
                .map(refs -> {
                    RefList.Builder<Ref> allRefs = new RefList.Builder<>();
                    RefList.Builder<Ref> onlySymbolicRefs = new RefList.Builder<>();

                    for (Ref ref : refs) {
                        allRefs.add(ref);

                        if (ref.isSymbolic())
                            onlySymbolicRefs.add(ref);
                    }

                    return new RefCache(allRefs.toRefList(), onlySymbolicRefs.toRefList());
                })
                .toBlocking()
                .lastOrDefault(new RefCache(RefList.emptyList(), RefList.emptyList()));
    }
}
