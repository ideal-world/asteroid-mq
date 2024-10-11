package com.github.RWDai;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.RWDai.Types.EdgeError;
import com.github.RWDai.Types.EdgePayload;
import com.github.RWDai.Types.EdgePushPayload;
import com.github.RWDai.Types.EdgeResponseEnum;
import com.github.RWDai.Types.EdgeResponsePayload;
import com.github.RWDai.Types.EdgeResult;

public class Node {
  private WebSocketClient socket;
  private AtomicInteger requestId = new AtomicInteger(0);
  private Map<Integer, CompletableFuture<EdgeResult<EdgeResponseEnum, EdgeError>>> responseWaitingPool = new HashMap<>();
  private Set<CompletableFuture<Void>> openingWaitingPool = new HashSet<>();
  private boolean alive = false;
  private Map<String, Endpoint> endpoints = new HashMap<>();
  private ObjectMapper objectMapper = new ObjectMapper();

  public static Node connect(String url) {
    Node node = new Node();
    try {
      WebSocketClient socket = new WebSocketClient(new URI(url)) {
        @Override
        public void onOpen(ServerHandshake handshakedata) {
          node.alive = true;
          // Todo
          node.openingWaitingPool.forEach(null);
        }

        @Override
        public void onMessage(String message) {
          // 实现 onMessage 逻辑
          try {
            EdgePayload payload = node.objectMapper.readValue(message, EdgePayload.class);
            if (payload instanceof EdgeResponsePayload) {
              var result = ((EdgeResponsePayload) payload).getContent().getResult();
              var seqId = ((EdgeResponsePayload) payload).getContent().getSeqId();
              var channel = node.responseWaitingPool.get(seqId);
              dgeResult<EdgeResponseEnum, EdgeError> result = payload.getContent().getResult();
              int seqId = payload.getContent().getSeqId();
              CompletableFuture<EdgeResult<EdgeResponseEnum, EdgeError>> future = responseWaitingPool.get(seqId);
              if (future != null) {
                future.complete(result);
              }
            } else if (payload instanceof EdgePushPayload) {

            } else {

            }
          } catch (JsonMappingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }

        }

        @Override
        public void onClose(int code, String reason, boolean remote) {
          node.alive = false;
        }

        @Override
        public void onError(Exception ex) {
          // 实现 onError 逻辑
          node.alive = false;
        }
      };
      socket.connect();
      node.socket = socket;
      return node;
    } catch (Exception e) {
      throw new RuntimeException("Failed to connect", e);
    }
  }

  private Node() {
  }

  private void ackMessage() {

  }

  private void sendPayload(EdgePayload payload) {
    try {
      String json = objectMapper.writeValueAsString(payload);
      socket.send(json);
    } catch (Exception e) {
      throw new RuntimeException("Failed to send payload", e);
    }
  }

  private int nextRequestId() {
    return requestId.incrementAndGet();
  }

  private CompletableFuture<EdgeResult<EdgeResponseEnum, EdgeError>> waitResponse(int requestId) {
    CompletableFuture<EdgeResult<EdgeResponseEnum, EdgeError>> future = new CompletableFuture<>();
    responseWaitingPool.put(requestId, future);
    return future;
  }

  private CompletableFuture<Void> waitSocketOpen() {
    if (alive) {
      return CompletableFuture.completedFuture(null);
    } else {
      CompletableFuture<Void> future = new CompletableFuture<>();
      openingWaitingPool.add(future);
      return future;
    }
  }

  // ... 其他方法的实现 ...

  public boolean isAlive() {
    return alive;
  }

  public void close() {
    // 实现关闭逻辑
  }
}
