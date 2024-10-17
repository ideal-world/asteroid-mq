package com.github.RWDai;

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;

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
    private Date expire;
    private Integer maxReceiver;

    public Date getExpire() {
      return expire;
    }

    public void setExpire(Date expire) {
      this.expire = expire;
    }

    public Integer getMaxReceiver() {
      return maxReceiver;
    }

    public void setMaxReceiver(Integer maxReceiver) {
      this.maxReceiver = maxReceiver;
    }

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
    private EdgeMessage content;

    public SendMessageRequest(EdgeMessage content) {
      this.content = content;
    }

    public EdgeMessage getContent() {
      return content;
    }
  }

  public static class EndpointOnlineRequest implements EdgeRequestEnum {
    private EdgeEndpointOnline content;

    public EndpointOnlineRequest(EdgeEndpointOnline content) {
      this.content = content;
    }

    public EdgeEndpointOnline getContent() {
      return content;
    }
  }

  public static class EndpointOfflineRequest implements EdgeRequestEnum {
    private EdgeEndpointOffline content;

    public EndpointOfflineRequest(EdgeEndpointOffline content) {
      this.content = content;
    }

    public EdgeEndpointOffline getContent() {
      return content;
    }
  }

  public static class EndpointInterestRequest implements EdgeRequestEnum {
    private EndpointInterest content;

    public EndpointInterestRequest(EndpointInterest content) {
      this.content = content;
    }

    public EndpointInterest getContent() {
      return content;
    }
  }

  public static class SetStateRequest implements EdgeRequestEnum {
    private SetState content;

    public SetStateRequest(SetState content) {
      this.content = content;
    }

    public SetState getContent() {
      return content;
    }
  }

  public static class EdgeRequest {
    private int seqId;
    private EdgeRequestEnum request;

    public EdgeRequest(int seqId, EdgeRequestEnum request) {
      this.seqId = seqId;
      this.request = request;
    }

    public int getSeqId() {
      return seqId;
    }

    public void setSeqId(int seqId) {
      this.seqId = seqId;
    }

    public EdgeRequestEnum getRequest() {
      return request;
    }

    public void setRequest(EdgeRequestEnum request) {
      this.request = request;
    }

  }

  public interface EdgeResult<T, E> {
  }

  public static class Ok<T, E> implements EdgeResult<T, E> {
    private T content;

    public T getContent() {
      return content;
    }

    public void setContent(T content) {
      this.content = content;
    }
  }

  public static class Err<T, E> implements EdgeResult<T, E> {
    private E content;

    public E getContent() {
      return content;
    }

    public void setContent(E content) {
      this.content = content;
    }

  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "kind")
  public static abstract class EdgeResponseEnum {
    protected String kind;

    public void setKind(String kind) {
      this.kind = kind;
    }
  }

  @JsonTypeName("SendMessage")
  public static class SendMessageResponse extends EdgeResponseEnum {
    private EdgeResult<WaitAckSuccess, WaitAckError> content;

    public SendMessageResponse(EdgeResult<WaitAckSuccess, WaitAckError> content) {
      this.content = content;
    }

    public String getKind() {
      return "SendMessage";
    }

    public EdgeResult<WaitAckSuccess, WaitAckError> getContent() {
      return content;
    }

    public void setContent(EdgeResult<WaitAckSuccess, WaitAckError> content) {
      this.content = content;
    }
  }

  @JsonTypeName("EndpointOnline")
  public static class EndpointOnlineResponse extends EdgeResponseEnum {
    private String content;

    public String getContent() {
      return content;
    }

    public void setContent(String content) {
      this.content = content;
    }
  }

  @JsonTypeName("EndpointOffline")
  public static class EndpointOfflineResponse extends EdgeResponseEnum {
  }

  @JsonTypeName("EndpointInterest")
  public static class EndpointInterestResponse extends EdgeResponseEnum {
  }

  @JsonTypeName("SetState")
  public static class SetStateResponse extends EdgeResponseEnum {
  }

  public static class EdgeResponse {
    private int seqId;
    private EdgeResult<EdgeResponseEnum, EdgeError> result;

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
    private String topicCode;
    private String endpoint;
    private List<String> interests;

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
    private String messageId;
    private MessageAckExpectKind ackKind;
    private MessageTargetKind targetKind;
    private MessageDurabilityConfig durability;
    private List<String> subjects;

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
    private String messageId;
    private Map<String, MessageStatusKind> status;

    public MessageStateUpdate(String messageId, Map<String, MessageStatusKind> status) {
      this.messageId = messageId;
      this.status = status;
    }

    public String getMessageId() {
      return messageId;
    }

    public void setMessageId(String messageId) {
      this.messageId = messageId;
    }

    public Map<String, MessageStatusKind> getStatus() {
      return status;
    }

    public void setStatus(Map<String, MessageStatusKind> status) {
      this.status = status;
    }

  }

  public static class SetState {
    private String topic;
    private MessageStateUpdate update;

    public SetState(String topic, MessageStateUpdate update) {
      this.topic = topic;
      this.update = update;
    }

    public String getTopic() {
      return topic;
    }

    public void setTopic(String topic) {
      this.topic = topic;
    }

    public MessageStateUpdate getUpdate() {
      return update;
    }

    public void setUpdate(MessageStateUpdate update) {
      this.update = update;
    }

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

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "kind")
  public static abstract class EdgePayload {
    protected String kind;

    public void setKind(String kind) {
      this.kind = kind;
    }
  }

  @JsonTypeName("Push")
  public static class EdgePushPayload extends EdgePayload {
    private EdgePush content;

    public EdgePushPayload(EdgePush content) {
      this.content = content;
    }

    public String getKind() {
      return "Push";
    }

    public EdgePush getContent() {
      return content;
    }

    public void setContent(EdgePush content) {
      this.content = content;
    }
  }

  @JsonTypeName("Response")
  public static class EdgeResponsePayload extends EdgePayload {
    private EdgeResponse content;

    public EdgeResponsePayload(EdgeResponse content) {
      this.content = content;
    }

    public String getKind() {
      return "Response";
    }

    public EdgeResponse getContent() {
      return content;
    }

    public void setContent(EdgeResponse content) {
      this.content = content;
    }
  }

  @JsonTypeName("Request")
  public static class EdgeRequestPayload extends EdgePayload {
    private EdgeRequest content;

    public EdgeRequestPayload(EdgeRequest content) {
      this.content = content;
    }

    public String getKind() {
      return "Request";
    }

    public EdgeRequest getContent() {
      return content;
    }

    public void setContent(EdgeRequest content) {
      this.content = content;
    }
  }

  @JsonTypeName("Error")
  public static class EdgeErrorPayload extends EdgePayload {
    private EdgeError content;

    public EdgeErrorPayload(EdgeError content) {
      this.content = content;
    }

    public String getKind() {
      return "Error";
    }

    public EdgeError getContent() {
      return content;
    }

    public void setContent(EdgeError content) {
      this.content = content;
    }
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "kind")
  public static abstract class EdgePush {
    protected String kind;

    public void setKind(String kind) {
      this.kind = kind;
    }
  }

  @JsonTypeName("Message")
  public static class MessagePush extends EdgePush {
    private List<String> endpoints;
    private Message message;

    public MessagePush(List<String> endpoints, Message message) {
      this.endpoints = endpoints;
      this.message = message;
    }

    public String getKind() {
      return "Message";
    }

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