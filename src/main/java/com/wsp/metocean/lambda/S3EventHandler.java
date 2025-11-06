package com.wsp.metocean.lambda;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.URLDecoder;
import java.net.http.HttpResponse; // kept only because your code referenced it elsewhere
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;

import com.azure.storage.blob.models.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

// === Azure SDK imports (new) ===
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import com.azure.storage.blob.*;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.net.URLDecoder;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

public class S3EventHandler implements RequestHandler<S3Event, String> {
    private static final Logger logger = LogManager.getLogger(S3EventHandler.class);
    // Reuse the S3 client across invocations (execution environment is reused by Lambda)
    private static final S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();

    public static void main(String... args) {
        S3EventHandler app = new S3EventHandler();

        // Create a dummy S3EventNotificationRecord
        S3EventNotification.S3Entity s3Entity = new S3EventNotification.S3Entity(
                null,
                new S3EventNotification.S3BucketEntity("test-bucket", null, null),
                new S3EventNotification.S3ObjectEntity("test-key", 123L, null, null, null),
                null
        );
        S3EventNotification.S3EventNotificationRecord record = new S3EventNotification.S3EventNotificationRecord(
                null, null, null, null, null, null, null, s3Entity, null
        );

        // Create S3Event with the record
        java.util.List<S3EventNotification.S3EventNotificationRecord> records = java.util.Collections.singletonList(record);
        S3Event event = new S3Event(records);

        // Create a dummy Context
        Context context = new Context() {
            @Override public String getAwsRequestId() {return "dummy";}

            @Override public String getLogGroupName() {return "dummy";}

            @Override public String getLogStreamName() {return "dummy";}

            @Override public String getFunctionName() {return "dummy";}

            @Override public String getFunctionVersion() {return "dummy";}

            @Override public String getInvokedFunctionArn() {return "dummy";}

            @Override public com.amazonaws.services.lambda.runtime.CognitoIdentity getIdentity() {return null;}

            @Override public com.amazonaws.services.lambda.runtime.ClientContext getClientContext() {return null;}

            @Override public int getRemainingTimeInMillis() {return 300000;}

            @Override public int getMemoryLimitInMB() {return 512;}

            @Override public LambdaLogger getLogger() {
                return new LambdaLogger() {
                    @Override
                    public void log(String message) {
                        System.out.println("LOG: " + message);
                    }

                    @Override
                    public void log(byte[] message) {
                        System.out.println("LOG: " + new String(message));
                    }
                };
            }
        };

        // Call handleRequest
        String result = app.handleRequest(event, context);
        System.out.println("Result: " + result);
    }

    @Override
    public String handleRequest(S3Event event, Context context) {
        if (event == null || event.getRecords() == null || event.getRecords().isEmpty()) {
            logger.warn("Received empty S3 event");
            return "OK";
        }
        boolean istesting = event.getRecords().size() == 1 && event.getRecords().get(0) != null && event.getRecords().get(0).getS3().getBucket().getName().equals("test-bucket");
        if (istesting) {
            String key = "Sites/qc/RQCSMRF-21/products/2025/12.zip";//s3://nimbus-clients/
            String azureFileNam = convertS3KeyToAzureFileName(key);
            String cameraStoragePath = "Imagescameras/"; // note the trailing space—remove if not intended
            String containerName = "depot";
            String connectionString =
                    "BlobEndpoint=https://sttrprosmrprod01cc.blob.core.windows.net;"
                            + "SharedAccessSignature=sv=2025-01-05&si=FournisseurDepot12025&sr=c&sig=ErtPiJiio4VH9jzYlU3z%2FoRNsV2a2361uQYDlfpPyMc%3D";
            String blobName = normalizePath(cameraStoragePath) + azureFileNam;
            System.out.println("source and desti: " + key + "\n" + azureFileNam + "\n" + containerName + "\n" + blobName);

String[] args2 = new String[] {
                    "--url", "https://sttrprosmrprod01cc.blob.core.windows.net/depot?sv=2025-01-05&si=FournisseurDepot12025&sr=c&sig=ErtPiJiio4VH9jzYlU3z%2FoRNsV2a2361uQYDlfpPyMc%3D",
                    "--name", blobName,
                    "--file", "/tmp/mtq_azure_zipping/README.md"
            };
            Map<String, String> a = parseArgs(args2);

            String url = required(a, "--url");
            String name = required(a, "--name");
            String file = a.get("--file");
            String text = a.get("--text");
            if ((file == null) == (text == null)) {
                die(2, "Provide exactly one of --file or --text");
            }

            boolean noOverwrite = a.containsKey("--no-overwrite");
            boolean sendMd5 = a.containsKey("--md5");
            double timeoutSec = a.containsKey("--timeout") ? Double.parseDouble(a.get("--timeout")) : 120.0;

            byte[] data;
            String contentType;
            if (file != null) {
                Path p = Path.of(file);
                if (!Files.isRegularFile(p)) die(2, "Not a file: " + file);
                try {
                    data = Files.readAllBytes(p);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                String guessed = null;
                try {
                    guessed = guessContentType(p);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                contentType = a.getOrDefault("--content-type", guessed);
            } else {
                data = text.getBytes(StandardCharsets.UTF_8);
                contentType = a.getOrDefault("--content-type", "text/plain; charset=UTF-8");
            }

            String blobUrl = buildBlobUrl(url, name); // preserves SAS query verbatim
            System.out.println("putBlob2 Uploading to: " + blobUrl);

            int rc = putBlob2(blobUrl, data, contentType, !noOverwrite, sendMd5, (int) Math.ceil(timeoutSec));
            System.exit(rc);



            // --- Build service & container clients from connection string ---
          /*  BlobServiceClient service = new BlobServiceClientBuilder().connectionString(connectionString).buildClient();
            BlobContainerClient container = service.getBlobContainerClient(containerName);
            if (!container.exists()) {
                container.create();
            }


            System.out.println("Listing blobs from container root:");
            for (var blobItem : container.listBlobs()) {
                System.out.println("Blob: " + blobItem.getName());
            }

            System.out.println("Listing blobs under: " + cameraStoragePath);
            for (var blobItem : container.listBlobsByHierarchy(cameraStoragePath)) {
                System.out.println("Blob: " + blobItem.getName());
            }*/

//            BlobClient blobClient = container.getBlobClient(blobName);
//            byte[] data = "Hello from Java with context!".getBytes(StandardCharsets.UTF_8);
//            ByteArrayInputStream dataStream = new ByteArrayInputStream(data);
//            Map<String, String> metadata = new HashMap<>();
//            metadata.put("uploadedBy", "mtq_azure_zipping");
//            metadata.put("purpose", "demo");
//
//            // --- Upload with a Context carrying a custom key/value ---
//            // Many SDK operations accept a Context via *WithResponse overloads on BlockBlobClient,
//            // but the simple upload on BlobClient forwards the Context as well when using the builder path.
//            // To be explicit, get a BlockBlobClient and call uploadWithResponse so we can pass Context directly.
//            var blockBlob = blobClient.getBlockBlobClient();
//            BlobHttpHeaders headers = null;
//            com.azure.storage.blob.models.BlobRequestConditions requestConditions = null;
//            com.azure.storage.blob.models.AccessTier tier = null;
//            java.time.Duration timeout = null;
//            var response = blockBlob.uploadWithResponse(
//                    dataStream,                          // InputStream data
//                    data.length,                         // long length
//                    headers,                             // BlobHttpHeaders headers
//                    metadata,                            // Map<String, String> metadata
//                    tier,                                // AccessTier tier
//                    null,                                // byte[] contentMd5
//                    requestConditions,                   // BlobRequestConditions requestConditions
//                    timeout,                             // Duration timeout
//                    new com.azure.core.util.Context("my-context-key", "my-context-value") // Context context
//            );
//            System.out.printf("Uploaded %s (ETag=%s, Status=%d)%n", blobName, response.getValue().getETag(), response.getStatusCode());
        } else {
            for (S3EventNotification.S3EventNotificationRecord r : event.getRecords()) {
                String bucket = r.getS3().getBucket().getName();
                String rawKey = r.getS3().getObject().getKey();
                String key = URLDecoder.decode(rawKey, StandardCharsets.UTF_8);
                HeadObjectResponse head = s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
                logger.info("Object size={} contentType={}", head.contentLength(), head.contentType());
                // Example: fetch up to first KB to prove access (avoid large downloads in Lambda)
                ResponseBytes<?> bytes = s3.getObjectAsBytes(GetObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .range("bytes=0-1023")
                        .build());
                logger.info("Read {} bytes from s3://{}/{}", bytes.asByteArray().length, bucket, key);
            }
        }
        return "OK";
    }

    public static String convertS3KeyToAzureFileName(String key) {
        // Example key: "Sites/qc/RQCSMRF-13/products/2025/09.zip"    2025_09_SMRF-13.zip
        String[] parts = key.split("/");
        if (parts.length < 5)
            return key; // fallback

        String site = parts[2]; // "RQCSMRF-13"
        String year = parts[4]; // "2025"
        String monthFile = parts[5]; // "09.zip"

        // Extract only the site code after the last '-'
        String[] siteParts = site.split("-");
        String siteCode = siteParts.length > 1 ? siteParts[1] : site;

        return year + "_" + monthFile.replace(".zip", "") + "_SMRF-" + siteCode + ".zip";
    }

    private static String normalizePath(String path) {
        if (path == null) return "";
        // Trim spaces and ensure single trailing slash if not empty
        String trimmed = path.trim();
        if (trimmed.isEmpty()) return "";
        return trimmed.endsWith("/") ? trimmed : trimmed + "/";
    }

    static String buildBlobUrl(String containerUrl, String blobName) {
        try {
            URI u = new URI(containerUrl.trim().replace("\"", "").replace("'", ""));
            String basePath = u.getPath().replaceAll("/$", ""); // no trailing slash
            String encoded = encodePathPreservingSlashes(blobName.replaceFirst("^/", ""));
            String newPath = basePath + "/" + encoded;
            return new URI(u.getScheme(), u.getAuthority(), newPath, u.getQuery(), u.getFragment()).toString();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid container URL: " + containerUrl, e);
        }
    }

    /** Encode path segments per RFC 3986 (keep '/' separators). */
    static String encodePathPreservingSlashes(String path) {
        String[] parts = path.split("/", -1);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) sb.append('/');
            sb.append(URLEncoder.encode(parts[i], StandardCharsets.UTF_8)
                    .replace("+", "%20")
                    .replace("%2F", "/"));
        }
        return sb.toString();
    }

    static String guessContentType(Path p) throws IOException {
        String ct = Files.probeContentType(p);
        return ct != null ? ct : "application/octet-stream";
    }

    /** Perform PUT with headers equivalent to your script. Returns 0 on success, 1 on failure. */
    static int putBlob(String blobUrl, byte[] data, String contentType, boolean overwrite, boolean sendMd5, int timeoutSec) {
        try {
            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(timeoutSec))
                    .build();

            HttpRequest.Builder rb = HttpRequest.newBuilder(URI.create(blobUrl))
                    .timeout(Duration.ofSeconds(timeoutSec))
                    .PUT(HttpRequest.BodyPublishers.ofByteArray(data))
                    .header("x-ms-blob-type", "BlockBlob")
                    // Your Python sets x-ms-version; it's optional when SAS (sv=...) is present, but we mirror it:
                    .header("x-ms-version", "2025-01-05") // mirrors your script comment/behavior
                    .header("Content-Type", contentType);

            if (!overwrite) {
                rb.header("If-None-Match", "*");
            }
            if (sendMd5) {
                rb.header("Content-MD5", computeMd5B64(data));
            }

            HttpResponse<byte[]> resp = client.send(rb.build(), HttpResponse.BodyHandlers.ofByteArray());
            int sc = resp.statusCode();
            if (sc == 201 || sc == 202) { // Created (usual) or Accepted
                String etag = header(resp, "ETag");
                String lm = header(resp, "Last-Modified");
                String md5h = header(resp, "Content-MD5");
                System.out.println("PUT OK");
                if (etag != null || lm != null || md5h != null) {
                    System.out.printf("ETag=%s Last-Modified=%s Content-MD5=%s%n", etag, lm, md5h);
                }
                return 0;
            } else {
                System.err.printf("HTTP %d%n", sc);
                String body = new String(resp.body(), StandardCharsets.UTF_8);
                System.err.println(body.substring(0, Math.min(body.length(), 600)));
                if (sc == 403) {
                    System.err.println("Hint: SAS may be missing 'c'/'w', expired, scope mismatch, or signature mutated.");
                }
                if (sc == 409 && !overwrite) {
                    System.err.println("Conflict (blob exists) and --no-overwrite was used. Remove the flag to overwrite.");
                }
                return 1;
            }
        } catch (Exception e) {
            e.printStackTrace(System.err);
            return 1;
        }
    }

    static int putBlob2(String blobUrl, byte[] data, String contentType, boolean overwrite, boolean sendMd5, int timeoutSec) {
        try {

            // Parse container URL and folder prefix from blobUrl
            URI uri = new URI(blobUrl);
            String[] pathParts = uri.getPath().split("/");
            if (pathParts.length < 3) throw new IllegalArgumentException("Invalid blobUrl: " + blobUrl);
            String containerName = pathParts[1];
            String blobPath = uri.getPath().substring(containerName.length() + 2); // skip leading slash and container
            String folderPrefix = blobPath.contains("/") ? blobPath.substring(0, blobPath.lastIndexOf('/') + 1) : "";

            // Rebuild container URL with SAS
            String containerUrl = uri.getScheme() + "://" + uri.getHost() + "/" + containerName + (uri.getQuery() != null ? "?" + uri.getQuery() : "");

            BlobContainerClient container = new BlobContainerClientBuilder()
                    .endpoint(containerUrl)
                    .buildClient();

            System.out.println("Listing blobs under: " + folderPrefix);
            for (BlobItem blobItem : container.listBlobsByHierarchy(folderPrefix)) {
                System.out.println("Blob: " + blobItem.getName());
            }


// Build a BlobClient, then get the BlockBlobClient
            BlobClient blobClient = new BlobClientBuilder().endpoint(blobUrl).buildClient();
            BlockBlobClient block = blobClient.getBlockBlobClient();

            BlobHttpHeaders headers = new BlobHttpHeaders().setContentType(contentType);

            BlobRequestConditions cond = overwrite ? null : new BlobRequestConditions().setIfNoneMatch("*");

            byte[] md5 = sendMd5 ? computeMd5Bytes(data) : null;

            // Optional: turn on string-to-sign logging for signature diagnostics during dev.
            com.azure.core.util.Context ctx = new com.azure.core.util.Context("Azure-Storage-Log-String-To-Sign", true);

            var resp = block.uploadWithResponse(
                    new ByteArrayInputStream(data), // InputStream
                    data.length,                    // length
                    headers,                        // BlobHttpHeaders
                    null,                           // metadata
                    null,                           // AccessTier
                    md5,                            // contentMd5
                    cond,                           // conditions
                    Duration.ofSeconds(timeoutSec),// timeout
                    ctx                             // context
            );

            int sc = resp.getStatusCode();
            if (sc == 201 || sc == 202) {
                System.out.println("PUT OK");
                String etag = resp.getHeaders().getValue("ETag");
                String lm   = resp.getHeaders().getValue("Last-Modified");
                String md5h = resp.getHeaders().getValue("Content-MD5");
                if (etag != null || lm != null || md5h != null) {
                    System.out.printf("ETag=%s Last-Modified=%s Content-MD5=%s%n", etag, lm, md5h);
                }
                return 0;
            } else {
                System.err.printf("HTTP %d%n", sc);
                return 1;
            }
        } catch (BlobStorageException bse) {
            System.err.printf("Azure error: %s (status %d)%n", bse.getErrorCode(), bse.getStatusCode());
            System.err.println(bse.getServiceMessage());
            if (bse.getStatusCode() == 403) {
                System.err.println("Hint: SAS may be missing 'c'/'w', expired, scope mismatch, or signature mutated.");
            }
            return 1;
        } catch (Exception e) {
            e.printStackTrace(System.err);
            return 1;
        }
    }

    private static byte[] computeMd5Bytes(byte[] data) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            return md5.digest(data);
        } catch (Exception e) {
            throw new RuntimeException("MD5 not available", e);
        }
    }

    static String computeMd5B64(byte[] data) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] dig = md5.digest(data);
            return Base64.getEncoder().encodeToString(dig);
        } catch (Exception e) {
            throw new RuntimeException("MD5 not available", e);
        }
    }

    static String header(HttpResponse<?> r, String name) {
        return r.headers().firstValue(name).orElse(null);
    }

    // --- tiny arg parser: --key value | flags ---
    static Map<String, String> parseArgs(String[] argv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < argv.length; i++) {
            String k = argv[i];
            if (!k.startsWith("--")) die(2, "Unexpected arg: " + k);
            if (i + 1 < argv.length && !argv[i + 1].startsWith("--")) {
                m.put(k, argv[++i]);
            } else {
                m.put(k, "true");
            }
        }
        return m;
    }

    static String required(Map<String, String> m, String key) {
        String v = m.get(key);
        if (v == null || v.isBlank()) die(2, "Missing required " + key);
        return v;
    }

    static void die(int code, String msg) {
        System.err.println(msg);
        System.err.println(USAGE);
        System.exit(code);
    }

    static final String USAGE = """
        Usage:
          java ... AzPutBlob --url <containerUrlWithSAS> --name <blobName> (--file <path> | --text <str>)
            [--content-type <type>] [--no-overwrite] [--md5] [--timeout <seconds>]

        Example:
          java -cp target/classes com.wsp.metocean.az.AzPutBlob \\
            --url 'https://sttrprosmrprod01cc.blob.core.windows.net/depot?sv=2025-01-05&si=FournisseurDepot12025&sr=c&sig=...' \\
            --name 'Imagescameras/2025-05_SMRF-00-test-12.zip' \\
            --file 2025-09_SMRF-01.zip --md5
        """;
}
