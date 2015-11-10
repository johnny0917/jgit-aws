package org.chodavarapu.jgitaws;

import com.amazonaws.services.s3.AmazonS3;
import org.chodavarapu.jgitaws.aws.DynamoClient;
import org.chodavarapu.jgitaws.repositories.ConfigurationRepository;
import org.chodavarapu.jgitaws.repositories.PackDescriptionRepository;
import org.chodavarapu.jgitaws.repositories.PackRepository;
import org.chodavarapu.jgitaws.repositories.RefRepository;

/**
 * @author Ravi Chodavarapu (rchodava@gmail.com)
 */
public class JGitAwsConfiguration {
    public static final String DEFAULT_CONFIGURATIONS_TABLE_NAME = "jga.Configurations";
    public static final String DEFAULT_REFS_TABLE_NAME = "jga.Refs";
    public static final String DEFAULT_PACK_DESCRIPTIONS_TABLE_NAME = "jga.PackDescriptions";
    private static final String DEFAULT_PACKS_BUCKET_NAME = "jga.Packs";

    private final DynamoClient dynamoClient;
    private final AmazonS3 s3Client;
    private final ConfigurationRepository configurationRepository;
    private final PackRepository packRepository;
    private final PackDescriptionRepository packDescriptionRepository;
    private final RefRepository refRepository;

    private String configurationsTableName = DEFAULT_CONFIGURATIONS_TABLE_NAME;
    private String packDescriptionsTableName = DEFAULT_PACK_DESCRIPTIONS_TABLE_NAME;
    private String packsBucketName = DEFAULT_PACKS_BUCKET_NAME;
    private String refsTableName = DEFAULT_REFS_TABLE_NAME;

    public JGitAwsConfiguration(DynamoClient dynamoClient, AmazonS3 s3Client) {
        this.dynamoClient = dynamoClient;
        this.s3Client = s3Client;

        this.configurationRepository = new ConfigurationRepository(this);
        this.packRepository = new PackRepository(this);
        this.packDescriptionRepository = new PackDescriptionRepository(this);
        this.refRepository = new RefRepository(this);
    }

    public String getPacksBucketName() {
        return packsBucketName;
    }

    public void setPacksBucketName(String packsBucketName) {
        this.packsBucketName = packsBucketName;
    }

    public PackRepository getPackRepository() {
        return packRepository;
    }

    public ConfigurationRepository getConfigurationRepository() {
        return configurationRepository;
    }

    public PackDescriptionRepository getPackDescriptionRepository() {
        return packDescriptionRepository;
    }

    public RefRepository getRefRepository() {
        return refRepository;
    }

    public DynamoClient getDynamoClient() {
        return dynamoClient;
    }

    public String getConfigurationsTableName() {
        return configurationsTableName;
    }

    public JGitAwsConfiguration setConfigurationsTableName(String name) {
        this.configurationsTableName = name;
        return this;
    }

    public String getPackDescriptionsTableName() {
        return packDescriptionsTableName;
    }

    public JGitAwsConfiguration setPackDescriptionsTableName(String packDescriptionsTableName) {
        this.packDescriptionsTableName = packDescriptionsTableName;
        return this;
    }

    public String getRefsTableName() {
        return refsTableName;
    }

    public JGitAwsConfiguration setRefsTableName(String name) {
        this.configurationsTableName = name;
        return this;
    }

    public AmazonS3 getS3Client() {
        return s3Client;
    }
}
