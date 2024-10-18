package com.github.RWDai;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.HashSet;

import org.junit.jupiter.api.Test;

public class ConnectionTest {
  public String getWsUrl() throws IOException, InterruptedException {
    HttpClient client = HttpClient.newHttpClient();
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:8080/node_id"))
        .PUT(HttpRequest.BodyPublishers.noBody())
        .build();
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
    return "ws://localhost:8080/connect?node_id=" + response.body();
  }

  @Test
  public void testConnection() throws IOException, InterruptedException {
    Node node_a = Node.connect(getWsUrl());

    Endpoint endpoint_b1 = node_a.createEndpoint("test", Arrays.asList("event/*"));
    endpoint_b1.updateInterests(new HashSet<>(Arrays.asList("event/hello")));
    Thread.startVirtualThread(() -> {
      while (true) {
        try {
          var message = endpoint_b1.nextMessage();
          System.out.println("Received message: " + message.getMessage().getPayload());
          message.ackProcessed();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });

    Types.EdgeMessageHeader header = new Types.EdgeMessageHeader(Types.MessageAckExpectKind.Sent,
        Types.MessageTargetKind.Push, null, Arrays.asList("event/hello", "event/hello/avatar/b2"), "test");
    node_a.sendMessage(new Types.EdgeMessage(header, "world"));
    node_a.sendMessage(new Types.EdgeMessage(header, "alice"));
    node_a.sendMessage(new Types.EdgeMessage(header, "bob"));
    Thread.sleep(1000L);
    node_a.close();
  }
}
