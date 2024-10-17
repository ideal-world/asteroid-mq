package com.github.RWDai;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class Endpoint {
  private Node node;
  private String topic;
  private Set<String> interests;
  private String address;
  // private final boolean isOnline;
  // private final List<ReceivedMessage> messageQueue;
  private CompletableFuture<ReceivedMessage> waitingNextMessage;

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

  public CompletableFuture<ReceivedMessage> getWaitingNextMessage() {
    return waitingNextMessage;
  }

  public void setWaitingNextMessage(CompletableFuture<ReceivedMessage> waitingNextMessage) {
    this.waitingNextMessage = waitingNextMessage;
  }

  public void offline() {
    node.destroyEndpoint(this);
  }

  public void closeMessageChannel() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'closeMessageChannel'");
  }

}
