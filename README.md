# Welcome to your CDK TypeScript project

This is a blank project for CDK development with TypeScript.

The `cdk.json` file tells the CDK Toolkit how to execute your app.

## Useful commands

- `npm run build` compile typescript to js
- `npm run watch` watch for changes and compile
- `npm run test` perform the jest unit tests
- `cdk deploy` deploy this stack to your default AWS account/region
- `cdk diff` compare deployed stack with current state
- `cdk synth` emits the synthesized CloudFormation template

## Process to deploy CDK (Yoshi's Macbook Pro Apple Silicon)

1. Follow [instruction to set up CDK](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html)

   - Install awscli via brew --> aws command failed --> Install six via brew
   - Install aws-cdk
   - `aws configure sso`
   - `cdk bootstrap aws://ACCOUNT-NUMBER/REGION` --> Error: "no credentials have been configured" --> Look into the hidden files ~/.aws/config and ~/.aws/credentials, then copy and paste the value from "Coomand line or programmatic access" popup SSO login screen.

2. Folow [instruction to deploy CDK](https://docs.aws.amazon.com/cdk/v2/guide/hello_world.html)
   - `cdk init app --language typescript`
   - `npm run build`
   - `cdk ls`
   - `cdk synth`
   - `cdk deploy`
