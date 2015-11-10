# jgit-aws [![Build Status](https://travis-ci.org/rchodava/jgit-aws.svg?branch=master)](https://travis-ci.org/rchodava/jgit-aws)
This is a (fairly naive) implementation of a JGit Git repository using Amazon DynamoDB and S3 as a store. It is built
on the JGit DfsRepository - and it is not particularly optimized.

## Storage
The repository uses a Dynamo table called jga.Refs to store Git refs, a Dynamo table called jga.Configurations to store
a repository's config, a single S3 bucket called jga.Packs to store pack file contents, and a Dynamo table called
jga.PackDescriptions to store meta-data about the pack files. All these names are configurable.
