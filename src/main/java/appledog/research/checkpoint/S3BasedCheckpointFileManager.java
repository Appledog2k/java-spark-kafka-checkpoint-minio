package appledog.research.checkpoint;

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.spark.sql.execution.streaming.CheckpointFileManager;

import java.util.ArrayList;
import java.util.List;

public class S3BasedCheckpointFileManager implements CheckpointFileManager {
    private static final String API_PATH_STYLE_ACCESS = "fs.s3a.path.style.access";
    private static final String SERVER_ENDPOINT = "fs.s3a.endpoint";
    private static final String SERVER_REGION = "fs.s3a.region";

    private final Path path;
    private final AmazonS3 s3Client;

    public S3BasedCheckpointFileManager(Path path, Configuration hadoopConfiguration) {
        this.path = path;

        boolean pathStyleAccess = "true".equals(hadoopConfiguration.get(API_PATH_STYLE_ACCESS, "true"));
        String endpoint = hadoopConfiguration.get(SERVER_ENDPOINT, "http://127.0.0.1:9000");
        String location = hadoopConfiguration.get(SERVER_REGION, "us-east-1");

        this.s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(Credentials.load(hadoopConfiguration))
                .withPathStyleAccessEnabled(pathStyleAccess)
                .withEndpointConfiguration(new EndpointConfiguration(endpoint, location))
                .build();
    }

    @Override
    public FileStatus[] list(Path path, PathFilter filter) {
        String p = path.toString().replaceFirst("^s3a://", "").trim();

        // Remove leading separator
        if (!p.isEmpty() && p.charAt(0) == Path.SEPARATOR_CHAR) {
            p = p.substring(1);
        }

        int objectPos = p.indexOf(Path.SEPARATOR_CHAR);
        String bucketName = p.substring(0, objectPos);
        String prefix = p.substring(objectPos + 1);

        List<FileStatus> results = new ArrayList<>();
        ObjectListing objectsResponse = s3Client.listObjects(bucketName, prefix);

        do {
            for (S3ObjectSummary s3Object : objectsResponse.getObjectSummaries()) {
                results.add(newFile(s3Object));
            }

            // Get next batch if the listing is truncated
            if (objectsResponse.isTruncated()) {
                objectsResponse = s3Client.listNextBatchOfObjects(objectsResponse);
            } else {
                break;
            }
        } while (true);

        return results.toArray(new FileStatus[0]);
    }

    private FileStatus newFile(S3ObjectSummary obj) {
        return new FileStatus(
                obj.getSize(),                    // length
                false,                            // isDirectory
                1,                                // block replication
                64 * 1024 * 1024,                // blockSize
                obj.getLastModified().getTime(),  // modificationTime
                new Path(obj.getBucketName(), obj.getKey()) // path
        );
    }

    @Override
    public void mkdirs(Path path) {
        // No-op for object storage
    }

    @Override
    public CancellableFSDataOutputStream createAtomic(Path path, boolean overwriteIfPossible) {
        String p = path.toString().replaceFirst("^s3a://", "").trim();

        // Remove leading separator
        if (!p.isEmpty() && p.charAt(0) == Path.SEPARATOR_CHAR) {
            p = p.substring(1);
        }

        int objectPos = p.indexOf(Path.SEPARATOR_CHAR);
        String bucketName = p.substring(0, objectPos);
        String objectName = p.substring(objectPos + 1);

        if (objectName.isEmpty()) {
            throw new IllegalArgumentException(path + " is not a valid path for the file system");
        }

        final S3OutputStream outputStream = new S3OutputStream(s3Client, bucketName, objectName);

        return new CancellableFSDataOutputStream(outputStream) {
            @Override
            public void cancel() {
                outputStream.cancel();
            }

            @Override
            public void close() {
                outputStream.close();
            }
        };
    }

    @Override
    public FSDataInputStream open(Path path) {
        String p = path.toString().replaceFirst("^s3a://", "").trim();

        // Remove leading separator
        if (!p.isEmpty() && p.charAt(0) == Path.SEPARATOR_CHAR) {
            p = p.substring(1);
        }

        int objectPos = p.indexOf(Path.SEPARATOR_CHAR);
        String bucketName = p.substring(0, objectPos);
        String objectName = p.substring(objectPos + 1);

        if (objectName.isEmpty()) {
            throw new IllegalArgumentException(path + " is not a valid path for the file system");
        }

        return new FSDataInputStream(new S3InputStream(s3Client, bucketName, objectName));
    }

    @Override
    public boolean exists(Path path) {
        String p = path.toString().replaceFirst("^s3a://", "").trim();

        // Remove leading separator
        if (!p.isEmpty() && p.charAt(0) == Path.SEPARATOR_CHAR) {
            p = p.substring(1);
        }

        int objectPos = p.indexOf(Path.SEPARATOR_CHAR);
        String bucketName = p.substring(0, objectPos);
        String objectName = p.substring(objectPos + 1);

        if (objectName.isEmpty()) {
            throw new IllegalArgumentException(path + " is not a valid path for the file system");
        }

        return s3Client.doesObjectExist(bucketName, objectName);
    }

    private boolean isEmpty(String str) {
        return str == null || str.trim().isEmpty();
    }

    @Override
    public void delete(Path path) {
        String p = path.toString().replaceFirst("^s3a://", "").trim();

        // Remove leading separator
        if (!p.isEmpty() && p.charAt(0) == Path.SEPARATOR_CHAR) {
            p = p.substring(1);
        }

        int objectPos = p.indexOf(Path.SEPARATOR_CHAR);
        String bucketName = p.substring(0, objectPos);
        String objectName = p.substring(objectPos + 1);

        try {
            ObjectMetadata objectMeta = s3Client.getObjectMetadata(bucketName, objectName);
            String versionId = objectMeta.getVersionId();

            if (isEmpty(versionId)) {
                // Always delete the latest for unversioned
                s3Client.deleteVersion(bucketName, objectName, "null");
            } else {
                // Always delete the latest
                s3Client.deleteVersion(bucketName, objectName, versionId);
            }
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() != 404) {
                throw e;
            }
        }
    }

    @Override
    public boolean isLocal() {
        return false;
    }

    @Override
    public Path createCheckpointDirectory() {
        // No need to create the checkpoints folder
        // subsequent commit/delta files automatically create this top level folder
        return path;
    }
}