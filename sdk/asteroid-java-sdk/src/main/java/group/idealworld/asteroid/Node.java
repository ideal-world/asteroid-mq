package group.idealworld.asteroid;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;

import group.idealworld.asteroid.EdgeException.NodeException;
import group.idealworld.asteroid.Types.EdgeEndpointOnline;
import group.idealworld.asteroid.Types.EdgeError;
import group.idealworld.asteroid.Types.EdgeMessage;
import group.idealworld.asteroid.Types.EdgePayload;
import group.idealworld.asteroid.Types.EdgePushPayload;
import group.idealworld.asteroid.Types.EdgeRequest;
import group.idealworld.asteroid.Types.EdgeRequestEnum;
import group.idealworld.asteroid.Types.EdgeResponseEnum;
import group.idealworld.asteroid.Types.EdgeResponsePayload;
import group.idealworld.asteroid.Types.EdgeResult;
import group.idealworld.asteroid.Types.MessageAck;
import group.idealworld.asteroid.Types.MessagePush;
import group.idealworld.asteroid.Types.MessageStateUpdate;
import group.idealworld.asteroid.Types.SendMessageRequest;
import group.idealworld.asteroid.Types.SetState;
import group.idealworld.asteroid.Types.SetStateRequest;

public class Node implements AutoCloseable {
  private static ObjectMapper objectMapper = new ObjectMapper()
      .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
      .setSerializationInclusion(JsonInclude.Include.NON_NULL)
      .setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX"))
      .setTimeZone(TimeZone.getTimeZone("UTC"));

  private static final Logger log = LoggerFactory.getLogger(Node.class);

  private WebSocketClient socket;
  private CountDownLatch socketConnectLatch = new CountDownLatch(1);;
  private AtomicInteger requestId = new AtomicInteger(0);
  private BlockingQueue<BoxRequest> requestPool = new LinkedBlockingQueue<>();
  private boolean alive = false;
  private Map<String, Endpoint> endpoints = new HashMap<>();
  private static Map<String, BlockingQueue<Types.Message>> tempMessageQueue = new HashMap<>();

  private Node() {
  }

  public boolean isAlive() {
    try {
      socketConnectLatch.await();
    } catch (InterruptedException e) {
      throw new NodeException("Failed to connect", e);
    }
    return alive;
  }

  private void setAlive(boolean alive) {
    this.alive = alive;
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

  public static class EdgeResponseHolder {
    private BlockingQueue<EdgeResult<EdgeResponseEnum, EdgeError>> responseQueue;

    private EdgeResponseHolder(BlockingQueue<EdgeResult<EdgeResponseEnum, EdgeError>> responseQueue) {
      this.responseQueue = responseQueue;
    }

    public EdgeResult<EdgeResponseEnum, EdgeError> await() throws InterruptedException {
      return responseQueue.take();
    }

  }

  protected Map<String, Endpoint> getEndpoints() {
    return endpoints;
  }

  public static Node connect(String url, Optional<Function<Exception, Void>> onError) {
    Node node = new Node();
    var responsePool = new HashMap<Integer, BlockingQueue<EdgeResult<EdgeResponseEnum, EdgeError>>>();
    try {
      WebSocketClient socket = new WebSocketClient(new URI(url)) {
        @Override
        public void onOpen(ServerHandshake handshakedata) {
          node.setAlive(true);
          node.socketConnectLatch.countDown();
          log.info("[Node] socket onOpen");
        }

        public void onMessage(ByteBuffer bytes) {
          String message = new String(bytes.array());
          onMessage(message);
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
                log.debug("[Node EdgeResponse] Response:{}", objectMapper.writeValueAsString(result));
                responseQueue.put(result);
              }
            } else if (payload instanceof EdgePushPayload) {
              var content = ((EdgePushPayload) payload).getContent();
              if (content instanceof MessagePush) {
                var contentMessage = ((MessagePush) content).getContent().getMessage();
                var contentEndpoints = ((MessagePush) content).getContent().getEndpoints();

                for (String endpointAddress : contentEndpoints) {
                  var endpoint = node.endpoints.get(endpointAddress);
                  if (endpoint == null) {
                    var tempQueue = tempMessageQueue.get(endpointAddress);
                    if (tempQueue == null) {
                      tempQueue = new LinkedBlockingQueue<>();
                      tempMessageQueue.put(endpointAddress, tempQueue);
                    }
                    tempQueue.put(contentMessage);
                    continue;
                  }
                  log.debug("[Node EdgePush] Received Push payload:{}",
                      objectMapper.writeValueAsString(contentMessage.getPayload()));
                  endpoint.getMessageQueue().put(contentMessage);
                }
              } else {
                log.warn("socket onMessage unknown payload:{}", objectMapper.writeValueAsString(payload));
              }
            } else {
              log.warn("socket onMessage unknown payload:{}", objectMapper.writeValueAsString(payload));
            }
          } catch (InterruptedException | JsonProcessingException e) {
            log.warn("socket onMessage error", e);
          }

        }

        @Override
        public void onClose(int code, String reason, boolean remote) {
          node.setAlive(false);
          node.socketConnectLatch.countDown();
          log.info("[Node] socket onClose");
        }

        @Override
        public void onError(Exception ex) {
          this.close();
          node.setAlive(false);
          node.socketConnectLatch.countDown();
          log.warn("[Node] onError", ex);
          onError.ifPresent(fn -> fn.apply(ex));
        }
      };
      socket.connect();

      Thread.ofVirtual().name("send-request-thread").start(() -> {
        while (true) {
          try {
            var boxRequest = node.getRequestPool().take();
            var seq_id = boxRequest.getRequest().getSeqId();
            log.debug("[Node sendRequest] Request: {}", objectMapper.writeValueAsString(boxRequest.getRequest()));
            var message = objectMapper.writeValueAsString(new Types.EdgeRequestPayload(boxRequest.getRequest()));
            log.trace("[Node sendRequest] json request: {}", message);
            responsePool.put(seq_id, boxRequest.getResponseQueue());
            socket.send(message.getBytes());
          } catch (InterruptedException | JsonProcessingException e) {
            log.warn("socket sendRequest error", e);
          }
        }
      });
      node.socket = socket;
      return node;
    } catch (URISyntaxException e) {
      throw new NodeException("Failed to connect", e);
    }
  }

  public EdgeResponseHolder sendMessage(EdgeMessage message) throws InterruptedException {
    if (!isAlive()) {
      throw new NodeException("Node is not alive");
    }
    return sendRequest(new SendMessageRequest(message));
  }

  public Endpoint createEndpoint(String topicCode, List<String> interests) throws InterruptedException {
    if (!isAlive()) {
      throw new NodeException("Node is not alive");
    }
    var address = sendEndpointsOnline(new EdgeEndpointOnline(topicCode, interests));
    var endpoint = new Endpoint(this, topicCode, new HashSet<>(interests), address);
    endpoints.put(address, endpoint);
    if (tempMessageQueue.containsKey(address)) {
      var tempQueue = tempMessageQueue.remove(address);
      while (!tempQueue.isEmpty()) {
        endpoint.getMessageQueue().put(tempQueue.take());
      }
    }
    log.info("[Node createEndpoint] endpoint: {}", endpoint);
    return endpoint;
  }

  private EdgeResponseHolder sendRequest(EdgeRequestEnum request) throws InterruptedException {
    var responseQueue = new LinkedBlockingQueue<EdgeResult<EdgeResponseEnum, EdgeError>>();
    BoxRequest boxRequest = new BoxRequest();
    boxRequest.setRequest(new EdgeRequest(nextRequestId(), request));
    boxRequest.setResponseQueue(responseQueue);
    requestPool.put(boxRequest);
    return new EdgeResponseHolder(responseQueue);
  }

  private int nextRequestId() {
    return requestId.incrementAndGet();
  }

  protected void sendSingleAck(MessageAck ack) throws InterruptedException {
    var response = sendRequest(
        new SetStateRequest(new SetState(ack.getTopicCode(), new MessageStateUpdate(ack.getAckTo(),
            Map.of(ack.getFrom(), ack.getKind())))))
        .await();
    if (response instanceof Types.Ok) {
      @SuppressWarnings("rawtypes")
      var content = ((Types.Ok) response).getContent();
      if (content instanceof Types.SetStateResponse) {
        return;
      }
    }
    throw new EdgeException.UnknownResponseException("Unknown response type");
  }

  private String sendEndpointsOnline(EdgeEndpointOnline request) throws InterruptedException {
    var response = sendRequest(new Types.EndpointOnlineRequest(request)).await();
    if (response instanceof Types.Ok) {
      @SuppressWarnings("rawtypes")
      var content = ((Types.Ok) response).getContent();
      if (content instanceof Types.EndpointOnlineResponse) {
        return ((Types.EndpointOnlineResponse) content).getContent();
      }
    }
    throw new EdgeException.UnknownResponseException("Unknown response type");
  }

  protected void sendEndpointsOffline(Types.EdgeEndpointOffline request) throws InterruptedException {
    var response = sendRequest(new Types.EndpointOfflineRequest(request)).await();
    if (response instanceof Types.Ok) {
      @SuppressWarnings("rawtypes")
      var content = ((Types.Ok) response).getContent();
      if (content instanceof Types.EndpointOfflineResponse) {
        return;
      }
    }
    throw new EdgeException.UnknownResponseException("Unknown response type");
  }

  protected void sendEndpointsInterests(Types.EndpointInterestRequest request) throws InterruptedException {
    var response = sendRequest(request).await();
    if (response instanceof Types.Ok) {
      @SuppressWarnings("rawtypes")
      var content = ((Types.Ok) response).getContent();
      if (content instanceof Types.EndpointInterestResponse) {
        return;
      }
    }
    throw new EdgeException.UnknownResponseException("Unknown response type");
  }

  @Override
  public void close() {
    var threads = new ArrayList<Thread>();
    // 实现关闭逻辑
    for (Endpoint ep : this.endpoints.values()) {
      threads.add(ep.closeEndpoint());
    }
    // 等待上面所有线程结束
    for (Thread thread : threads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        log.warn("close endpoint error", e);
      }
    }
    socket.close();
  }
}
