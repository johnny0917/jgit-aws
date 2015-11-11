package org.chodavarapu.jgitaws;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import org.chodavarapu.jgitaws.jgit.AmazonRepository;
import org.eclipse.jgit.errors.RepositoryNotFoundException;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.storage.pack.PackConfig;
import org.eclipse.jgit.transport.Daemon;
import org.eclipse.jgit.transport.DaemonClient;
import org.eclipse.jgit.transport.ServiceMayNotContinueException;
import org.eclipse.jgit.transport.resolver.RepositoryResolver;
import org.eclipse.jgit.transport.resolver.ServiceNotAuthorizedException;
import org.eclipse.jgit.transport.resolver.ServiceNotEnabledException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * @author Ravi Chodavarapu (rchodava@gmail.com)
 */
public class GitTesting {
    private static final class RepositoryResolverImplementation implements
            RepositoryResolver<DaemonClient> {
        private JGitAwsConfiguration configuration;

        @Override
        public Repository open(DaemonClient client, String name)
                throws RepositoryNotFoundException,
                ServiceNotAuthorizedException, ServiceNotEnabledException,
                ServiceMayNotContinueException {
            AmazonRepository repo = repositories.get(name);
            if (repo == null) {
                try {
                    repo = new AmazonRepository.Builder()
                            .setRepositoryDescription(new DfsRepositoryDescription(name))
                            .setConfiguration(configuration)
                            .build();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                repositories.put(name, repo);
            }
            return repo;
        }
    }

    private static Map<String, AmazonRepository> repositories = new HashMap<>();

    public static void main(String[] args) throws IOException {
        PackConfig packConfig = new PackConfig();
        int threads = Runtime.getRuntime().availableProcessors();
        packConfig.setExecutor(Executors.newFixedThreadPool(threads));

        AmazonDynamoDB dynamoClient = new AmazonDynamoDBClient(new BasicAWSCredentials("test", "secret"));
        dynamoClient.setEndpoint("http://localhost:8000");

        AmazonS3 s3Client = new AmazonS3Client(new AnonymousAWSCredentials());
        s3Client.setEndpoint("http://localhost:4567");

        RepositoryResolverImplementation resolver = new RepositoryResolverImplementation();
        resolver.configuration = new JGitAwsConfiguration(dynamoClient, s3Client);

        Daemon server = new Daemon(new InetSocketAddress("localhost", 9418));
        server.setTimeout(5);
        server.setPackConfig(packConfig);
        server.getService("receive-pack").setEnabled(true);
        server.getService("receive-pack").setOverridable(true);
        server.setRepositoryResolver(resolver);
        server.start();
    }
}
