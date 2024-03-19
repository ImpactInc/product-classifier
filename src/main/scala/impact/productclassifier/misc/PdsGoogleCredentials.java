package impact.productclassifier.misc;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import lombok.Getter;


public class PdsGoogleCredentials {
    @Getter
    private final CredentialsProvider credentialsProvider;

    public PdsGoogleCredentials(String credentialsFile) throws IOException {
        final Credentials credentials;
        if (StringUtils.isBlank(credentialsFile)) {
            credentials = GoogleCredentials.getApplicationDefault();
        } else {
            File file = new File(credentialsFile);
            // Check file system & then classpath
            credentials = ServiceAccountCredentials.fromStream(Files.newInputStream(file.toPath()));
//            credentials = ServiceAccountCredentials.fromStream(file.exists() ?
//                    Files.newInputStream(file.toPath()) : new ClassPathResource(credentialsFile).getInputStream());
        }

        credentialsProvider = new FixedCredentialsProvider() {

            @Nullable
            @Override
            public Credentials getCredentials() {
                return credentials;
            }
        };
    }
}
