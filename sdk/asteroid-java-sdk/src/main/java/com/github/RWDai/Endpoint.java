package com.github.RWDai;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.RWDai.Types.Message;

public class Endpoint implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(Endpoint.class);

  private Node node;
  private String topic;
  private Set<String> interests;
  private String address;
  private BlockingQueue<Types.Message> messageQueue = new LinkedBlockingQueue<>();

  public static class EndpointReceivedMessage {
    private String address;
    private String topic;
    private Node node;
    private Message message;

    public EndpointReceivedMessage(Endpoint endpoint, Message message) {
      this.address = endpoint.getAddress();
      this.topic = endpoint.getTopic();
      this.node = endpoint.getNode();
      this.message = message;
    }

    public String getAddress() {
      return address;
    }

    public String getTopic() {
      return topic;
    }

    public Node getNode() {
      return node;
    }

    public Message getMessage() {
      return message;
    }

    public void ackFailed() {
      var ack = this.message.getHeader().ackFailed(this.topic, this.address);
      try {
        this.node.sendSingleAck(ack);
      } catch (InterruptedException e) {
        log.error("[Endpoint ackFailed] topic:{},address:{},error:{}", topic, address, e);
        throw new EdgeException.EndpointException(e);
      }
    }

    public void ackProcessed() {
      var ack = this.message.getHeader().ackProcessed(this.topic, this.address);
      try {
        this.node.sendSingleAck(ack);
      } catch (InterruptedException e) {
        log.error("[Endpoint ackProcessed] topic:{},address:{},error:{}", topic, address, e);
        throw new EdgeException.EndpointException(e);
      }
    }

    public void ackReceived() {
      var ack = this.message.getHeader().ackReceived(this.topic, this.address);
      try {
        this.node.sendSingleAck(ack);
      } catch (InterruptedException e) {
        log.error("[Endpoint ackReceived] topic:{},address:{},error:{}", topic, address, e);
        throw new EdgeException.EndpointException(e);
      }
    }
  }

  public Endpoint(Node node, String topic, Set<String> interests, String address) {
    this.node = node;
    this.topic = topic;
    this.interests = interests;
    this.address = address;
  }

  public Node getNode() {
    return node;
  }

  public void setNode(Node node) {
    this.node = node;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public Set<String> getInterests() {
    return interests;
  }

  public void setInterests(Set<String> interests) {
    this.interests = interests;
  }

  public String getAddress() {
    return address;
  }

  public void setAddress(String address) {
    this.address = address;
  }

  public BlockingQueue<Types.Message> getMessageQueue() {
    return messageQueue;
  }

  public void setMessageQueue(BlockingQueue<Types.Message> messageQueue) {
    this.messageQueue = messageQueue;
  }

  public void modifyInterests(Consumer<Set<String>> modify) {
    Set<String> newInterests = new HashSet<>(interests);
    modify.accept(newInterests);
    this.updateInterests(newInterests);
  }

  public void updateInterests(Set<String> newInterests) {
    try {
      this.node.sendEndpointsInterests(
          new Types.EndpointInterestRequest(new Types.EndpointInterest(topic, address, new ArrayList<>(newInterests))));
      this.interests = newInterests;
    } catch (InterruptedException e) {
      log.error("[Endpoint updateInterests] topic:{},address:{},error:{}", topic, address, e);
    }
  }

  public Optional<EndpointReceivedMessage> nextMessage() {
    try {
      Types.Message message = messageQueue.take();
      if (message instanceof Types.PoisonMessage) {
        return Optional.empty();
      }
      return Optional.of(new EndpointReceivedMessage(this, message));
    } catch (InterruptedException e) {
      log.error("[Endpoint nextMessage] topic:{},address:{},error:{}", topic, address, e);
      throw new EdgeException.EndpointException(e);
    }
  }

  @Override
  public void close() {
    closeEndpoint();
  }

  protected Thread closeEndpoint() {
    return Thread.startVirtualThread(() -> {
      try {
        node.sendEndpointsOffline(new Types.EdgeEndpointOffline(topic, address));
        node.getEndpoints().remove(address);
        messageQueue.put(new Types.PoisonMessage());
        log.info("[Endpoint offline] topic:{},address:{}", topic, address);
      } catch (InterruptedException e) {
        log.error("[Endpoint offline] topic:{},address:{},error:{}", topic, address, e);
      }
    });
  }

}
