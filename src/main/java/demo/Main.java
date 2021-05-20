package demo;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.GetSourceRequest;
import org.elasticsearch.client.core.GetSourceResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Predicate;

import static java.util.Arrays.stream;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.client.RequestOptions.DEFAULT;

public class Main {
    private static final String INDEX = "notes";
    private static RestHighLevelClient client;

    public static void main(String[] args) throws Exception {
        try (RestHighLevelClient c = client()) {
            client = c;
            test();
        }
    }

    private static void test() throws Exception {
        index("1", "Hello!");
        indexAsync("2", "Hi async!");
        System.out.println(get("1"));
        System.out.println(getSource("1"));
        System.out.println(exists("1"));
        delete("1");
        System.out.println(exists("1"));
        update("2", "Hi updated!");
        System.out.println(get("2"));
        upsert("3", "Hi upserted!");
        System.out.println(get("3"));
        upsert("3", "Hi upserted new!");
        System.out.println(get("3"));
        System.out.println(searchAll());
        bulkIndex(Map.of("4", "BulK item!", "5", "Another bulK item!", "6", "One more bulK item!"));
        Thread.sleep(1000);
        System.out.println(searchAll());

        System.out.println("OK!");
    }

    private static RestHighLevelClient client() {
        return new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
    }

    private static Map<String, Object> getSource(String id) throws IOException {
        GetSourceRequest request = new GetSourceRequest(INDEX, id);
        GetSourceResponse response = client.getSource(request, DEFAULT);
        System.out.println("GET SOURCE RESPONSE: " + response);
        return response.getSource();
    }

    private static List<Map<String, Object>> searchAll() throws IOException {
        List<Map<String, Object>> all = new ArrayList<>();
        SearchRequest request = new SearchRequest(INDEX);
        SearchResponse response = client.search(request, DEFAULT);
        System.out.println("SEARCH RESPONSE: " + response);
        response.getHits().forEach(hit -> all.add(hit.getSourceAsMap()));
        return all;
    }

    private static Map<String, Object> get(String id) throws IOException {
        GetRequest request = new GetRequest(INDEX, id);
        GetResponse response = client.get(request, DEFAULT);
        System.out.println("GET RESPONSE: " + response);
        return response.isExists() ? response.getSourceAsMap() : null;
    }

    private static boolean exists(String id) throws IOException {
        GetRequest request = new GetRequest(INDEX, id);
        boolean response = client.exists(request, DEFAULT);
        System.out.println("EXISTS RESPONSE: " + response);
        return response;
    }

    private static void delete(String id) throws IOException {
        DeleteRequest request = new DeleteRequest(INDEX, id);
        DeleteResponse response = client.delete(request, DEFAULT);
        System.out.println("DELETE RESPONSE: " + response);
    }

    private static void index(String id, String note) throws IOException {
        IndexRequest request = new IndexRequest(INDEX);
        request.id(id);
        request.source(singletonMap("text", note));
        IndexResponse response = client.index(request, DEFAULT);
        System.out.println("INDEX RESPONSE: " + response);
    }

    private static void bulkIndex(Map<String, String> map) {
        BulkProcessor bulkProcessor = bulkProcessor();
        map.forEach((id, note) -> {
            IndexRequest request = new IndexRequest(INDEX);
            request.id(id);
            request.source(singletonMap("text", note));
            bulkProcessor.add(request);
        });
        bulkProcessor.flush();
    }

    private static BulkProcessor.Listener bulkListener() {
        return new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                System.out.println("BEFORE BULK: " + request);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                List<String> succeeded = stream(response.getItems())
                        .filter(Predicate.not(BulkItemResponse::isFailed))
                        .map(BulkItemResponse::getId)
                        .collect(toList());
                List<String> failed = stream(response.getItems())
                        .filter(BulkItemResponse::isFailed)
                        .map(BulkItemResponse::getId)
                        .collect(toList());
                System.out.println("BULK SUCCEEDED: " + succeeded);
                System.out.println("BULK FAILED: " + failed);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                List<String> failed = request.requests().stream()
                        .map(DocWriteRequest::id)
                        .collect(toList());
                System.out.println("BULK FAILED " + failure + ": " + failed);
            }
        };
    }

    private static BulkProcessor bulkProcessor() {
        return BulkProcessor.builder((bulkRequest, actionListener) -> {
            try {
                actionListener.onResponse(client.bulk(bulkRequest, DEFAULT));
            } catch (Exception e) {
                actionListener.onFailure(e);
            }
        }, bulkListener()).build();
    }

    private static void update(String id, String note) throws IOException {
        UpdateRequest request = new UpdateRequest(INDEX, id)
                .doc(singletonMap("text", note));
        UpdateResponse response = client.update(request, DEFAULT);
        System.out.println("UPDATE RESPONSE: " + response);
    }

    private static void upsert(String id, String note) throws IOException {
        Map<String, Object> source = singletonMap("text", note);
        UpdateRequest request = new UpdateRequest(INDEX, id)
                .doc(source)
                .upsert(source);
        UpdateResponse response = client.update(request, DEFAULT);
        System.out.println("UPSERT RESPONSE: " + response);
    }

    private static void indexAsync(String id, String note) throws InterruptedException {
        IndexRequest request = new IndexRequest(INDEX);
        request.id(id);
        request.source(singletonMap("text", note));

        CountDownLatch latch = new CountDownLatch(1);
        client.indexAsync(request, DEFAULT, new ActionListener<>() {
            @Override
            public void onResponse(IndexResponse response) {
                System.out.println("INDEX RESPONSE: " + response);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                System.out.println("INDEX FAILURE: " + e);
                latch.countDown();
            }
        });

        latch.await();
    }
}
