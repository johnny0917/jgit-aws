# jgit-aws [![Build Status](https://travis-ci.org/rchodava/jgit-aws.svg?branch=master)](https://travis-ci.org/rchodava/jgit-aws)
This is a (fairly naive) implementation of a JGit Git repository using Amazon DynamoDB and S3 as a store. It is built
on the JGit `DfsRepository` - and it is not particularly optimized.

## Usage
Usage is fairly straighforward - create a new instance of the `AmazonRepository` using the `AmazonRepository.Builder`,
and passing in a `JGitAwsConfiguration` setup the way you like.

## Storage
The repository uses a Dynamo table called `jga.Refs` to store Git refs, a Dynamo table called `jga.Configurations` to store
a repository's config, a single S3 bucket called `jga.Packs` to store pack file contents, and a Dynamo table called
`jga.PackDescriptions` to store meta-data about the pack files. All these names are configurable.

## Configuration
You can configure the Dynamo table names, as well as the S3 bucket name by setting the appropriate properties on
`JGitAwsConfiguration`. Note that you can also configure the initial provisioned throughput used on the Dynamo tables
using `JGitAwsConfiguration` - by default they are all set up for 1 read unit and 1 write unit. Scaling this up is up
to you to do manually or using a tool such as Dynamic DyanmoDB.
