package com.github.RWDai;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class Types {

  public static class EdgeEndpointOffline {
    public String topicCode;
    public String endpoint;
  }

  public static class EdgeEndpointOnline {
    public String topicCode;
    /**
     * # Interest
     * ## Glob Match Interest
     * (/)?(<path>|<*>|<**>)/*
     */
    public List<String> interests;
  }

  public enum EdgeErrorKind {
    Decode,
    TopicNotFound,
    EndpointNotFound,
    Unauthorized,
    Internal
  }

  public static class EdgeError {
    public String context;
    public String message;
    public EdgeErrorKind kind;
  }

  public enum MessageAckExpectKind {
    Sent,
    Received,
    Processed
  }

  public enum MessageTargetKind {
    Durable,
    Online,
    Available,
    Push
  }

  public static class MessageDurabilityConfig {
    public Date expire;
    public Integer maxReceiver;
  }

  public static class EdgeMessageHeader {
    private MessageAckExpectKind ackKind;
    private MessageTargetKind targetKind;
    private MessageDurabilityConfig durability;
    private List<String> subjects;
    private String topic;

    public EdgeMessageHeader(MessageAckExpectKind ackKind, MessageTargetKind targetKind,
        MessageDurabilityConfig durability, List<String> subjects, String topic) {
      this.ackKind = ackKind;
      this.targetKind = targetKind;
      this.durability = durability;
      this.subjects = subjects;
      this.topic = topic;
    }

    public MessageAckExpectKind getAckKind() {
      return ackKind;
    }

    public MessageTargetKind getTargetKind() {
      return targetKind;
    }

    public MessageDurabilityConfig getDurability() {
      return durability;
    }

    public List<String> getSubjects() {
      return subjects;
    }

    public String getTopic() {
      return topic;
    }

    public void setAckKind(MessageAckExpectKind ackKind) {
      this.ackKind = ackKind;
    }

    public void setTargetKind(MessageTargetKind targetKind) {
      this.targetKind = targetKind;
    }

    public void setDurability(MessageDurabilityConfig durability) {
      this.durability = durability;
    }

    public void setSubjects(List<String> subjects) {
      this.subjects = subjects;
    }

    public void setTopic(String topic) {
      this.topic = topic;
    }
  }

  public static class EdgeMessage {
    private EdgeMessageHeader header;
    private String payload;

    public EdgeMessage(EdgeMessageHeader header, String payload) {
      this.header = header;
      this.payload = payload;
    }

    public EdgeMessageHeader getHeader() {
      return header;
    }

    public String getPayload() {
      return payload;
    }

    public void setHeader(EdgeMessageHeader header) {
      this.header = header;
    }

    public void setPayload(String payload) {
      this.payload = payload;
    }
  }

  public interface EdgeRequestEnum {
  }

  public static class SendMessageRequest implements EdgeRequestEnum {
    public EdgeMessage content;
  }

  public static class EndpointOnlineRequest implements EdgeRequestEnum {
    public EdgeEndpointOnline content;
  }

  public static class EndpointOfflineRequest implements EdgeRequestEnum {
    public EdgeEndpointOffline content;
  }

  public static class EndpointInterestRequest implements EdgeRequestEnum {
    public EndpointInterest content;
  }

  public static class SetStateRequest implements EdgeRequestEnum {
    public SetState content;
  }

  public static class EdgeRequest {
    public int seqId;
    public EdgeRequestEnum request;
  }

  public interface EdgeResult<T, E> {
  }

  public static class Ok<T, E> implements EdgeResult<T, E> {
    public T content;
  }

  public static class Err<T, E> implements EdgeResult<T, E> {
    public E content;
  }

  public interface EdgeResponseEnum {
  }

  public static class SendMessageResponse implements EdgeResponseEnum {
    EdgeResult<WaitAckSuccess, WaitAckError> content;

    public EdgeResult<WaitAckSuccess, WaitAckError> getContent() {
      return content;
    }

    public void setContent(EdgeResult<WaitAckSuccess, WaitAckError> content) {
      this.content = content;
    }
  }

  public static class EndpointOnlineResponse implements EdgeResponseEnum {
    String content;

    public String getContent() {
      return content;
    }

    public void setContent(String content) {
      this.content = content;
    }
  }

  public static class EndpointOfflineResponse implements EdgeResponseEnum {
  }

  public static class EndpointInterestResponse implements EdgeResponseEnum {
  }

  public static class SetStateResponse implements EdgeResponseEnum {
  }

  public static class EdgeResponse {
    int seqId;
    EdgeResult<EdgeResponseEnum, EdgeError> result;

    public int getSeqId() {
      return seqId;
    }

    public void setSeqId(int seqId) {
      this.seqId = seqId;
    }

    public EdgeResult<EdgeResponseEnum, EdgeError> getResult() {
      return result;
    }

    public void setResult(EdgeResult<EdgeResponseEnum, EdgeError> result) {
      this.result = result;
    }
  }

  public static class EndpointInterest {
    String topicCode;
    String endpoint;
    List<String> interests;

    public String getTopicCode() {
      return topicCode;
    }

    public void setTopicCode(String topicCode) {
      this.topicCode = topicCode;
    }

    public String getEndpoint() {
      return endpoint;
    }

    public void setEndpoint(String endpoint) {
      this.endpoint = endpoint;
    }

    public List<String> getInterests() {
      return interests;
    }

    public void setInterests(List<String> interests) {
      this.interests = interests;
    }
  }

  public static class MessageHeader {
    String messageId;
    MessageAckExpectKind ackKind;
    MessageTargetKind targetKind;
    MessageDurabilityConfig durability;
    List<String> subjects;

    public String getMessageId() {
      return messageId;
    }

    public void setMessageId(String messageId) {
      this.messageId = messageId;
    }

    public MessageAckExpectKind getAckKind() {
      return ackKind;
    }

    public void setAckKind(MessageAckExpectKind ackKind) {
      this.ackKind = ackKind;
    }

    public MessageTargetKind getTargetKind() {
      return targetKind;
    }

    public void setTargetKind(MessageTargetKind targetKind) {
      this.targetKind = targetKind;
    }

    public MessageDurabilityConfig getDurability() {
      return durability;
    }

    public void setDurability(MessageDurabilityConfig durability) {
      this.durability = durability;
    }

    public List<String> getSubjects() {
      return subjects;
    }

    public void setSubjects(List<String> subjects) {
      this.subjects = subjects;
    }
  }

  public static class Message {
    public MessageHeader header;
    public String payload;
  }

  public enum MessageStatusKind {
    Sending,
    Unsent,
    Sent,
    Received,
    Processed,
    Failed,
    Unreachable
  }

  public static class MessageStateUpdate {
    public String messageId;
    public Map<String, MessageStatusKind> status;
  }

  public static class SetState {
    public String topic;
    public MessageStateUpdate update;
  }

  public enum WaitAckErrorException {
    MessageDropped,
    Overflow,
    NoAvailableTarget
  }

  public static class WaitAckError {
    public Map<String, MessageStatusKind> status;
    public WaitAckErrorException exception;
  }

  public static class WaitAckSuccess {
    public Map<String, MessageStatusKind> status;
  }

  public interface EdgePayload {
  }

  public static class EdgePushPayload implements EdgePayload {
    EdgePush content;

    public EdgePush getContent() {
      return content;
    }

    public void setContent(EdgePush content) {
      this.content = content;
    }
  }

  public static class EdgeResponsePayload implements EdgePayload {
    EdgeResponse content;

    public EdgeResponse getContent() {
      return content;
    }

    public void setContent(EdgeResponse content) {
      this.content = content;
    }
  }

  public static class EdgeRequestPayload implements EdgePayload {
    EdgeRequest content;

    public EdgeRequest getContent() {
      return content;
    }

    public void setContent(EdgeRequest content) {
      this.content = content;
    }
  }

  public static class EdgeErrorPayload implements EdgePayload {
    EdgeError content;

    public EdgeError getContent() {
      return content;
    }

    public void setContent(EdgeError content) {
      this.content = content;
    }
  }

  public interface EdgePush {
  }

  public static class MessagePush implements EdgePush {
    List<String> endpoints;
    Message message;

    public List<String> getEndpoints() {
      return endpoints;
    }

    public void setEndpoints(List<String> endpoints) {
      this.endpoints = endpoints;
    }

    public Message getMessage() {
      return message;
    }

    public void setMessage(Message message) {
      this.message = message;
    }
  }
}