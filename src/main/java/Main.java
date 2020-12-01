import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.message.MessageWriter;
import io.cloudevents.http.HttpMessageFactory;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * @author Ali Ok (ali.ok@apache.org)
 * 01/12/2020 14:18
 **/
public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        final String sinkUrl = Optional.ofNullable(System.getenv("K_SINK")).orElseThrow(() -> new IllegalArgumentException("K_SINK is not set in the environment"));

        CloseableHttpClient client = HttpClients.createDefault();

        for (int i = 0; ; i++) {
            URL url = new URL(sinkUrl);
            HttpURLConnection httpUrlConnection = (HttpURLConnection) url.openConnection();
            httpUrlConnection.setRequestMethod("POST");
            httpUrlConnection.setDoOutput(true);

            final CloudEvent event = CloudEventBuilder.v1()
                    .withId("" + i)
                    .withSource(URI.create("/sample"))
                    .withType("tr.com.aliok.sample")
                    .withDataContentType("application/json")
                    .withData(("{ \"msg\" : \"hello\" " + i + " }").getBytes(StandardCharsets.UTF_8))
                    .build();

            System.out.println("Emitting event #" + i);

            HttpPost httpPost = new HttpPost(sinkUrl);

            MessageWriter messageWriter = createMessageWriter(client, httpPost);
            messageWriter.writeBinary(event);

            Thread.sleep(10000L);
        }
    }

    private static MessageWriter createMessageWriter(CloseableHttpClient httpClient, HttpPost httpPost) {
        return HttpMessageFactory.createWriter(
                httpPost::setHeader,
                bytes -> {
                    httpPost.setEntity(new ByteArrayEntity(bytes));
                    try {
                        httpClient.execute(httpPost);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
        );
    }

}
