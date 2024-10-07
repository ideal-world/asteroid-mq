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
    public EdgeResult<WaitAckSuccess, WaitAckError> content;
  }

  public static class EndpointOnlineResponse implements EdgeResponseEnum {
    public String content;
  }

  public static class EndpointOfflineResponse implements EdgeResponseEnum {
  }

  public static class EndpointInterestResponse implements EdgeResponseEnum {
  }

  public static class SetStateResponse implements EdgeResponseEnum {
  }

  public static class EdgeResponse {
    public int seqId;
    public EdgeResult<EdgeResponseEnum, EdgeError> result;
  }

  public static class EndpointInterest {
    public String topicCode;
    public String endpoint;
    public List<String> interests;
  }

  public static class MessageHeader {
    public String messageId;
    public MessageAckExpectKind ackKind;
    public MessageTargetKind targetKind;
    public MessageDurabilityConfig durability;
    public List<String> subjects;
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
    public EdgePush content;
  }

  public static class EdgeResponsePayload implements EdgePayload {
    public EdgeResponse content;
  }

  public static class EdgeRequestPayload implements EdgePayload {
    public EdgeRequest content;
  }

  public static class EdgeErrorPayload implements EdgePayload {
    public EdgeError content;
  }

  public interface EdgePush {
  }

  public static class MessagePush implements EdgePush {
    public List<String> endpoints;
    public Message message;
  }
}