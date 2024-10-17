package com.github.RWDai;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Endpoint implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(Endpoint.class);

  private Node node;
  private String topic;
  private Set<String> interests;
  private String address;
  // private final boolean isOnline;
  // private final List<ReceivedMessage> messageQueue;
  // private CompletableFuture<ReceivedMessage> waitingNextMessage;

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

  // public CompletableFuture<ReceivedMessage> getWaitingNextMessage() {
  // return waitingNextMessage;
  // }

  // public void setWaitingNextMessage(CompletableFuture<ReceivedMessage>
  // waitingNextMessage) {
  // this.waitingNextMessage = waitingNextMessage;
  // }

  // public void offline() {
  // node.destroyEndpoint(this);
  // }

  @Override
  public void close() {
    Thread.startVirtualThread(() -> {
      node.sendEndpointsOffline(topic, address);
      node.getEndpoints().remove(address);
      log.info("[Endpoint offline] topic:{},address:{}", topic, address);
    });
  }

}
