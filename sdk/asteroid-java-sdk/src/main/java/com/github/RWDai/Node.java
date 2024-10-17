package com.github.RWDai;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.RWDai.Types.EdgeEndpointOnline;
import com.github.RWDai.Types.EdgeError;
import com.github.RWDai.Types.EdgeMessage;
import com.github.RWDai.Types.EdgePayload;
import com.github.RWDai.Types.EdgePushPayload;
import com.github.RWDai.Types.EdgeRequest;
import com.github.RWDai.Types.EdgeRequestEnum;
import com.github.RWDai.Types.EdgeRequestPayload;
import com.github.RWDai.Types.EdgeResponseEnum;
import com.github.RWDai.Types.EdgeResponsePayload;
import com.github.RWDai.Types.EdgeResult;
import com.github.RWDai.Types.MessagePush;
import com.github.RWDai.Types.MessageStateUpdate;
import com.github.RWDai.Types.MessageStatusKind;
import com.github.RWDai.Types.SendMessageRequest;
import com.github.RWDai.Types.SetState;
import com.github.RWDai.Types.SetStateRequest;

public class Node implements AutoCloseable {
  private static ObjectMapper objectMapper = new ObjectMapper();

  private WebSocketClient socket;
  private AtomicInteger requestId = new AtomicInteger(0);
  // private Map<Integer, CompletableFuture<EdgeResult<EdgeResponseEnum,
  // EdgeError>>> responseWaitingPool = new HashMap<>();
  // private Set<CompletableFuture<Void>> openingWaitingPool = new HashSet<>();
  private BlockingQueue<BoxRequest> requestPool = new LinkedBlockingQueue<>();
  private boolean alive = false;
  private Map<String, Endpoint> endpoints = new HashMap<>();

  private Node() {
  }

  protected BlockingQueue<BoxRequest> getRequestPool() {
    return this.requestPool;
  }

  public static class BoxRequest {
    private EdgeRequest request;
    private BlockingQueue<EdgeResult<EdgeResponseEnum, EdgeError>> responseQueue;

    public EdgeRequest getRequest() {
      return request;
    }

    public void setRequest(EdgeRequest request) {
      this.request = request;
    }

    public BlockingQueue<EdgeResult<EdgeResponseEnum, EdgeError>> getResponseQueue() {
      return responseQueue;
    }

    public void setResponseQueue(BlockingQueue<EdgeResult<EdgeResponseEnum, EdgeError>> responseQueue) {
      this.responseQueue = responseQueue;
    }
  }

  protected Map<String, Endpoint> getEndpoints() {
    return endpoints;
  }

  public static Node connect(String url) {
    Node node = new Node();
    var responsePool = new HashMap<Integer, BlockingQueue<EdgeResult<EdgeResponseEnum, EdgeError>>>();
    try {
      WebSocketClient socket = new WebSocketClient(new URI(url)) {
        @Override
        public void onOpen(ServerHandshake handshakedata) {
          node.alive = true;
          // Todo

        }

        @Override
        public void onMessage(String message) {
          // 实现 onMessage 逻辑
          try {
            EdgePayload payload = objectMapper.readValue(message, EdgePayload.class);
            if (payload instanceof EdgeResponsePayload) {
              var result = ((EdgeResponsePayload) payload).getContent().getResult();
              var seqId = ((EdgeResponsePayload) payload).getContent().getSeqId();
              var responseQueue = responsePool.get(seqId);
              if (responseQueue != null) {
                responseQueue.add(result);
              }
            } else if (payload instanceof EdgePushPayload) {
              var content = ((EdgePushPayload) payload).getContent();
              if (content instanceof MessagePush) {
                var contentMessage = ((MessagePush) content).getMessage();
                var contentEndpoints = ((MessagePush) content).getEndpoints();

                for (String endpointName : contentEndpoints) {
                  var endpoint = node.endpoints.get(endpointName);
                  if (endpoint == null) {
                    continue;
                  }
                  // TODO
                }
              } else {

              }
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
          ex.printStackTrace();
        }
      };
      socket.connect();

      Thread.startVirtualThread(() -> {
        while (true) {
          try {
            var boxRequest = node.getRequestPool().take();
            var seq_id = boxRequest.getRequest().getSeqId();
            var message = objectMapper.writeValueAsString(new Types.EdgeRequestPayload(boxRequest.getRequest()));
            socket.send(message);
            responsePool.put(seq_id, boxRequest.getResponseQueue());
          } catch (InterruptedException | JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
      });
      node.socket = socket;
      return node;
    } catch (Exception e) {
      throw new RuntimeException("Failed to connect", e);
    }
  }

  public EdgeResult<EdgeResponseEnum, EdgeError> sendMessage(EdgeMessage message) throws InterruptedException {
    if (!alive) {
      throw new RuntimeException("Node is not alive");
    }
    return sendRequest(new SendMessageRequest(message));
  }

  private EdgeResult<EdgeResponseEnum, EdgeError> sendRequest(EdgeRequestEnum request) throws InterruptedException {
    var responseQueue = new LinkedBlockingQueue<EdgeResult<EdgeResponseEnum, EdgeError>>();
    BoxRequest boxRequest = new BoxRequest();
    boxRequest.setRequest(new EdgeRequest(nextRequestId(), request));
    boxRequest.setResponseQueue(responseQueue);
    requestPool.put(boxRequest);
    var result = responseQueue.take();
    return result;
  }

  private void ackMessage(Endpoint endpoint, String messageId, MessageStatusKind state) throws JsonProcessingException {
    if (alive) {
      // TODO
    }
    var requestId = nextRequestId();
    var request = new EdgeRequest(requestId, new SetStateRequest(
        new SetState(endpoint.getTopic(), new MessageStateUpdate(messageId, Map.of(endpoint.getAddress(), state)))));
    var message = new EdgeRequestPayload(request);
    this.sendPayload(message);
    return;
  }

  private void sendPayload(EdgePayload payload) throws JsonProcessingException {
    String json = objectMapper.writeValueAsString(payload);
    socket.send(json.getBytes());
  }

  private int nextRequestId() {
    return requestId.incrementAndGet();
  }

  // private CompletableFuture<EdgeResult<EdgeResponseEnum, EdgeError>>
  // waitResponse(int requestId) {
  // CompletableFuture<EdgeResult<EdgeResponseEnum, EdgeError>> future = new
  // CompletableFuture<>();
  // responseWaitingPool.put(requestId, future);
  // return future;
  // }

  // private CompletableFuture<Void> waitSocketOpen() {
  // if (alive) {
  // return CompletableFuture.completedFuture(null);
  // } else {
  // CompletableFuture<Void> future = new CompletableFuture<>();
  // openingWaitingPool.add(future);
  // return future;
  // }
  // }

  private String sendEndpointsOnline(EdgeEndpointOnline request) throws InterruptedException {
    var response = sendRequest(new Types.EndpointOnlineRequest(request));
    if (response instanceof Types.Ok) {
      var content = ((Types.Ok) response).getContent();
      if (content instanceof Types.EndpointOnlineResponse) {
        return ((Types.EndpointOnlineResponse) content).getContent();
      }
    } else {
      // TODO handle error
    }
    return null;
  }

  protected void sendEndpointsOffline(Types.EdgeEndpointOffline request) throws InterruptedException {
    var response = sendRequest(new Types.EndpointOfflineRequest(request));
    if (response instanceof Types.Ok) {
      var content = ((Types.Ok) response).getContent();
      if (content instanceof Types.EndpointOfflineResponse) {
        return;
      }
    } else {
      // TODO handle error
    }
    return;
  }

  protected void sendEndpointsInterests(Types.EndpointInterestRequest request) throws InterruptedException {
    var response = sendRequest(request);
    if (response instanceof Types.Ok) {
      var content = ((Types.Ok) response).getContent();
      if (content instanceof Types.EndpointInterestResponse) {
        return;
      }
    } else {
      // TODO handle error
    }
    return;
  }

  @Override
  public void close() {
    // 实现关闭逻辑
    for (Endpoint ep : this.endpoints.values()) {
      ep.close();
    }
    socket.close();
  }
}
