package appledog.research.checkpoint;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import org.apache.hadoop.conf.Configuration;

public class Credentials {
    /**
     * Creates a static credentials provider for given AWS credentials.
     *
     * @param credentials AWS credentials to be provided
     * @return AWSCredentialsProvider that provides the given static credentials
     */
    private static AWSCredentialsProvider staticCredentialsProvider(final AWSCredentials credentials) {
        return new AWSCredentialsProvider() {
            @Override
            public AWSCredentials getCredentials() {
                return credentials;
            }

            @Override
            public void refresh() {
                // No refresh needed for static credentials
            }
        };
    }

    /**
     * Loads AWS credentials from Hadoop configuration or falls back to default provider chain.
     *
     * @param hadoopConfiguration Hadoop configuration containing potential AWS credentials
     * @return AWSCredentialsProvider that can provide the appropriate credentials
     */
    public static AWSCredentialsProvider load(Configuration hadoopConfiguration) {
        String accessKey = hadoopConfiguration.get("fs.s3a.access.key");
        String secretKey = hadoopConfiguration.get("fs.s3a.secret.key");
        String sessionToken = hadoopConfiguration.get("fs.s3a.session.token");

        // If access key and secret key are provided in configuration
        if (accessKey != null && secretKey != null) {
            // Create session credentials if session token is available
            if (sessionToken != null) {
                return staticCredentialsProvider(
                        new BasicSessionCredentials(accessKey, secretKey, sessionToken));
            }
            // Otherwise create basic credentials
            return staticCredentialsProvider(
                    new BasicAWSCredentials(accessKey, secretKey));
        }

        // Fall back to default credentials provider chain if no explicit credentials provided
        return new DefaultAWSCredentialsProviderChain();
    }
}