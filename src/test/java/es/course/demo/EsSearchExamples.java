package es.course.demo;

import static org.elasticsearch.client.RequestOptions.DEFAULT;
import static org.elasticsearch.common.xcontent.XContentType.JSON;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.junit.jupiter.api.Test;

class EsSearchExamples {

  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
      .setSerializationInclusion(Include.NON_NULL)
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      .configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);

  public static RestHighLevelClient client = new RestHighLevelClient(
      RestClient.builder(new HttpHost("localhost", 9200)));

  public static final String SAMPLE_DATA_INDEX_NAME = "kibana_sample_data_ecommerce";

  @Test
  void checkThatIndexExists() throws IOException {
    var indices = client.indices().exists(new GetIndexRequest(SAMPLE_DATA_INDEX_NAME), DEFAULT);
    System.out.println(indices);
  }

  @Test
  void createUsersDataset() throws Exception {
    var indexName = "sample_users";

    var indicesClient = client.indices();
    var createIndexRequest = new CreateIndexRequest(indexName)
        .mapping(indexMappings())
        .settings(indexSettings());

    var createIndexResponse = indicesClient.create(createIndexRequest, DEFAULT);
    System.out.println("is index created: " + createIndexResponse.isAcknowledged());

    var bulkRequest = new BulkRequest().setRefreshPolicy(RefreshPolicy.IMMEDIATE);
    for (var userSample : createUserSampleDataset()) {
      var indexRequest = new IndexRequest(indexName)
          .id(userSample.getUsername())
          .source(OBJECT_MAPPER.writeValueAsString(userSample), JSON);
      bulkRequest.add(indexRequest);
    }

    var bulkResponse = client.bulk(bulkRequest, DEFAULT);
    Arrays.stream(bulkResponse.getItems()).forEach(e -> System.out.println(e.getResponse()));

    var searchSourceBuilder = searchSource()
        .query(termQuery("type", "admin"))
        .trackTotalHits(true);

    var searchResponse = client.search(new SearchRequest(indexName).source(searchSourceBuilder), DEFAULT);

    System.out.println("\nrequest query: \n" + OBJECT_MAPPER.readTree(searchSourceBuilder.toString()).toPrettyString());
    System.out.println("\nresult json: \n" + OBJECT_MAPPER.readTree(searchResponse.toString()).toPrettyString());

    System.out.println("\nfirst found document as map: " + searchResponse.getHits().getHits()[0].getSourceAsMap());
    System.out.println("total hits: " + searchResponse.getHits().getTotalHits().value);

    var deleteIndexResponse = client.indices().delete(new DeleteIndexRequest(indexName), DEFAULT);
    System.out.println("is index deleted: " + deleteIndexResponse.isAcknowledged());
  }

  private static List<UserSample> createUserSampleDataset() {
    return List.of(
        UserSample.of("user001", "Diogo", "Crowther", "user"),
        UserSample.of("user002", "Aden ", "Scott", "user"),
        UserSample.of("user003", "Ava-Mai", "Anderson", "admin"),
        UserSample.of("user004", "Milo", "Kaye", "user"),
        UserSample.of("user005", "Melisa", "English", "user"),
        UserSample.of("user006", "Arnold", "Simpson", "admin"),
        UserSample.of("user007", "Amber-Rose", "Andrew", "user"),
        UserSample.of("user008", "Timur", "Cottrell", "user"),
        UserSample.of("user009", "Beau", "Lyons", "admin"),
        UserSample.of("user010", "Izzie", "Wong", "user"),
        UserSample.of("user010", "James", null, "user")
    );
  }

  private static Map<String, Object> indexMappings() {
    return Map.of(
        "properties", Map.of(
            "username", Map.of("type", "keyword"),
            "firstName", Map.of("type", "keyword"),
            "lastName", Map.of("type", "keyword"),
            "type", Map.of("type", "keyword")
        )
    );
  }

  private static Map<String, Object> indexSettings() {
    return Map.of(
        "index", Map.of(
            "number_of_shards", 4,
            "number_of_replicas", 2,
            "refresh_interval", "20s"
        )
    );
  }

  @Data
  @AllArgsConstructor(staticName = "of")
  public static class UserSample {

    private String username;
    private String firstName;
    private String lastName;
    private String type;
  }
}
