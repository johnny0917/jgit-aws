package org.chodavarapu.jgitaws.git;

import org.eclipse.jgit.internal.storage.dfs.*;
import org.eclipse.jgit.internal.storage.pack.PackExt;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * @author Ravi Chodavarapu (rchodava@gmail.com)
 */
public class S3ObjectDatabase extends DfsObjDatabase {
    public S3ObjectDatabase(AmazonRepository repository, DfsReaderOptions readerOptions) {
        super(repository, readerOptions);
    }

    @Override
    protected void commitPackImpl(Collection<DfsPackDescription> desc, Collection<DfsPackDescription> replaces) throws IOException {

    }

    @Override
    protected List<DfsPackDescription> listPacks() throws IOException {
        return null;
    }

    @Override
    protected ReadableChannel openFile(DfsPackDescription desc, PackExt ext) throws IOException {
        return null;
    }

    @Override
    protected DfsPackDescription newPack(PackSource source) throws IOException {
        return null;
    }

    @Override
    protected void rollbackPack(Collection<DfsPackDescription> desc) {

    }

    @Override
    protected DfsOutputStream writeFile(DfsPackDescription desc, PackExt ext) throws IOException {
        return null;
    }
}
