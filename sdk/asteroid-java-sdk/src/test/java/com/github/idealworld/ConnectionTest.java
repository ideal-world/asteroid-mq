package com.github.idealworld;

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
    Node node_b = Node.connect(getWsUrl());

    Endpoint endpoint_b1 = node_a.createEndpoint("test", Arrays.asList("event/*"));
    Endpoint endpoint_b2 = node_b.createEndpoint("test", Arrays.asList("event/**/b2"));
    endpoint_b1.updateInterests(new HashSet<>(Arrays.asList("event/hello")));
    var task_b1 = Thread.startVirtualThread(() -> {
      while (true) {
        try {
          var message = endpoint_b1.nextMessage();
          if (message.isPresent()) {
            System.out.println("Received message in b1: " + message.get().getMessage().getPayload());
            message.get().ackProcessed();
          } else {
            break;
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    var task_b2 = Thread.startVirtualThread(() -> {
      while (true) {
        try {
          var message = endpoint_b2.nextMessage();
          if (message.isPresent()) {
            System.out.println("Received message in b2: " + message.get().getMessage().getPayload());
            message.get().ackProcessed();
          } else {
            break;
          }
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
    node_b.close();
    task_b1.join();
    task_b2.join();
  }
}
