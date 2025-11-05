# mtq_azure_zipping

**AWS Lambda (Java 17, AWS SDK v2) – S3 trigger**

GroupId: `com.wsp.metocean` · ArtifactId: `mtq_azure_zipping`

This project is a minimal, production‑ready scaffold for an AWS Lambda function that is triggered by S3 object creation events. It targets the Java 17 managed runtime and uses the AWS SDK for Java v2.

---

## Build

```bash
mvn -DskipTests package
```

This will produce a shaded JAR under `target/mtq_azure_zipping-0.1.0-SNAPSHOT-shaded.jar` suitable for direct upload to Lambda.

## Local unit tests

```bash
mvn test
```

## Deploy (AWS SAM — optional)

The included `template.yaml` lets you deploy the function and the S3 event mapping with [AWS SAM](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/what-is-sam.html).

```bash
sam build   # uses Maven under the hood
sam deploy --guided       --stack-name mtq-azure-zipping       --capabilities CAPABILITY_IAM       --parameter-overrides BucketName=<your-bucket-name>
```

## Configure S3 trigger (Console/Infra-as-Code)

If you are not using SAM, create an Event Notification on your bucket that publishes `s3:ObjectCreated:*` to this Lambda. Ensure the function role has `s3:GetObject`, `s3:ListBucket`, and `s3:GetObjectVersion` permissions for the source bucket/prefix.

## Handler

* **Class**: `com.wsp.metocean.lambda.S3EventHandler`
* **Method**: `handleRequest`
* **Runtime**: `java17`
* **Example handler string**: `com.wsp.metocean.lambda.S3EventHandler::handleRequest`

The handler logs the bucket/key, fetches object metadata, and reads up to the first KB as a quick proof of access. Replace with your business logic as needed.

## Notes

* AWS SDK v2 modules are imported via the BOM; adjust the property `aws.sdk.version` in `pom.xml` to your approved version if required.
* Log configuration is provided via `src/main/resources/log4j2.xml`.
* For large files, remove or modify the example `GetObject` call to avoid unnecessary downloads.
