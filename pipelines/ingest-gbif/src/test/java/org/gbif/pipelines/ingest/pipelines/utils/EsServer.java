package org.gbif.pipelines.ingest.pipelines.utils;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.gbif.pipelines.estools.client.EsClient;
import org.gbif.pipelines.estools.client.EsConfig;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.rules.ExternalResource;

import lombok.extern.slf4j.Slf4j;
import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic;
import pl.allegro.tech.embeddedelasticsearch.PopularProperties;

/**
 * ES server used for testing purposes.
 *
 * <p>This class is intended to be used as a {@link org.junit.ClassRule}.
 */
@Slf4j
public class EsServer extends ExternalResource {

  private static final String CLUSTER_NAME = "test_EScluster";

  private EmbeddedElastic embeddedElastic;
  private EsConfig esConfig;
  // needed to assert results against ES server directly
  private RestClient restClient;
  // I create both clients not to expose the restClient in EsClient
  private EsClient esClient;

  @Override
  protected void before() throws Throwable {
    embeddedElastic =
        EmbeddedElastic.builder()
            .withElasticVersion("6.5.4")
            .withEsJavaOpts("-Xms128m -Xmx512m")
            .withSetting(PopularProperties.HTTP_PORT, getAvailablePort())
            .withSetting(PopularProperties.TRANSPORT_TCP_PORT, getAvailablePort())
            .withSetting(PopularProperties.CLUSTER_NAME, CLUSTER_NAME)
            .withStartTimeout(120, TimeUnit.SECONDS)
            .build();

    embeddedElastic.start();

    esConfig = EsConfig.from(getServerAddress());
    restClient = buildRestClient();
    esClient = EsClient.from(esConfig);
  }

  @Override
  protected void after() {
    embeddedElastic.stop();
    esClient.close();
    try {
      restClient.close();
    } catch (IOException e) {
      log.error("Could not close rest client for testing", e);
    }
  }

  private static int getAvailablePort() throws IOException {
    ServerSocket serverSocket = new ServerSocket(0);
    int port = serverSocket.getLocalPort();
    serverSocket.close();

    return port;
  }

  String getServerAddress() {
    return "http://localhost:" + embeddedElastic.getHttpPort();
  }

  private RestClient buildRestClient() {
    HttpHost host = new HttpHost("localhost", embeddedElastic.getHttpPort());
    return RestClient.builder(host).build();
  }

  private String getEsVersion() throws IOException {
    Properties properties = new Properties();
    properties.load(this.getClass().getClassLoader().getResourceAsStream("maven.properties"));
    return properties.getProperty("elasticsearch.version");
  }

  public EsConfig getEsConfig() {
    return esConfig;
  }

  public RestClient getRestClient() {
    return restClient;
  }

  public EsClient getEsClient() {
    return esClient;
  }
}
