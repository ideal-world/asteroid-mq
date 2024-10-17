package com.github.RWDai;

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;

public class Types {

  public static class EdgeEndpointOffline {
    private String topicCode;
    private String endpoint;

    public EdgeEndpointOffline() {
    }

    public EdgeEndpointOffline(String topicCode, String endpoint) {
      this.topicCode = topicCode;
      this.endpoint = endpoint;
    }

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

  }

  public static class EdgeEndpointOnline {
    private String topicCode;
    /**
     * # Interest
     * ## Glob Match Interest
     * (/)?(<path>|<*>|<**>)/*
     */
    private List<String> interests;

    public EdgeEndpointOnline() {
    }

    public EdgeEndpointOnline(String topicCode, List<String> interests) {
      this.topicCode = topicCode;
      this.interests = interests;
    }

    public String getTopicCode() {
      return topicCode;
    }

    public void setTopicCode(String topicCode) {
      this.topicCode = topicCode;
    }

    public List<String> getInterests() {
      return interests;
    }

    public void setInterests(List<String> interests) {
      this.interests = interests;
    }

  }

  public enum EdgeErrorKind {
    Decode,
    TopicNotFound,
    EndpointNotFound,
    Unauthorized,
    Internal
  }

  public static class EdgeError {
    private String context;
    private String message;
    private EdgeErrorKind kind;

    public EdgeError() {
    }

    public EdgeError(String context, String message, EdgeErrorKind kind) {
      this.context = context;
      this.message = message;
      this.kind = kind;
    }

    public String getContext() {
      return context;
    }

    public void setContext(String context) {
      this.context = context;
    }

    public String getMessage() {
      return message;
    }

    public void setMessage(String message) {
      this.message = message;
    }

    public EdgeErrorKind getKind() {
      return kind;
    }

    public void setKind(EdgeErrorKind kind) {
      this.kind = kind;
    }

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

    public MessageDurabilityConfig() {
    }

    public MessageDurabilityConfig(Date expire, Integer maxReceiver) {
      this.expire = expire;
      this.maxReceiver = maxReceiver;
    }

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

    public EdgeMessageHeader() {
    }

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

    public EdgeMessage() {
    }

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

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "kind")
  public static abstract class EdgeRequestEnum {
    private String kind;

    public EdgeRequestEnum() {
    }

    public EdgeRequestEnum(String kind) {
      this.kind = kind;
    }

    public void setKind(String kind) {
      this.kind = kind;
    }
  }

  @JsonTypeName("SendMessage")
  public static class SendMessageRequest extends EdgeRequestEnum {
    private EdgeMessage content;

    public SendMessageRequest() {
    }

    public SendMessageRequest(EdgeMessage content) {
      super("SendMessage");
      this.content = content;
    }

    public String getKind() {
      return "SendMessage";
    }

    public EdgeMessage getContent() {
      return content;
    }

    public void setContent(EdgeMessage content) {
      this.content = content;
    }
  }

  @JsonTypeName("EndpointOnline")
  public static class EndpointOnlineRequest extends EdgeRequestEnum {
    private EdgeEndpointOnline content;

    public EndpointOnlineRequest() {
    }

    public EndpointOnlineRequest(EdgeEndpointOnline content) {
      super("EndpointOnline");
      this.content = content;
    }

    public String getKind() {
      return "EndpointOnline";
    }

    public EdgeEndpointOnline getContent() {
      return content;
    }

    public void setContent(EdgeEndpointOnline content) {
      this.content = content;
    }
  }

  @JsonTypeName("EndpointOffline")
  public static class EndpointOfflineRequest extends EdgeRequestEnum {
    private EdgeEndpointOffline content;

    public EndpointOfflineRequest() {
    }

    public EndpointOfflineRequest(EdgeEndpointOffline content) {
      super("EndpointOffline");
      this.content = content;
    }

    public String getKind() {
      return "EndpointOffline";
    }

    public EdgeEndpointOffline getContent() {
      return content;
    }

    public void setContent(EdgeEndpointOffline content) {
      this.content = content;
    }
  }

  @JsonTypeName("EndpointInterest")
  public static class EndpointInterestRequest extends EdgeRequestEnum {
    private EndpointInterest content;

    public EndpointInterestRequest() {
    }

    public EndpointInterestRequest(EndpointInterest content) {
      super("EndpointInterest");
      this.content = content;
    }

    public String getKind() {
      return "EndpointInterest";
    }

    public EndpointInterest getContent() {
      return content;
    }

    public void setContent(EndpointInterest content) {
      this.content = content;
    }

  }

  @JsonTypeName("SetState")
  public static class SetStateRequest extends EdgeRequestEnum {
    private SetState content;

    public SetStateRequest() {
    }

    public SetStateRequest(SetState content) {
      super("SetState");
      this.content = content;
    }

    public String getKind() {
      return "SetState";
    }

    public SetState getContent() {
      return content;
    }

    public void setContent(SetState content) {
      this.content = content;
    }

  }

  public static class EdgeRequest {
    private int seqId;
    private EdgeRequestEnum request;

    public EdgeRequest() {
    }

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

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "kind")
  public static abstract class EdgeResult<T, E> {
    protected String kind;

    public EdgeResult() {
    }

    public EdgeResult(String kind) {
      this.kind = kind;
    }

    public void setKind(String kind) {
      this.kind = kind;
    }
  }

  @JsonTypeName("Ok")
  public static class Ok<T, E> extends EdgeResult<T, E> {
    private T content;

    public Ok() {
    }

    public Ok(T content) {
      super("Ok");
      this.content = content;
    }

    public T getContent() {
      return content;
    }

    public void setContent(T content) {
      this.content = content;
    }
  }

  @JsonTypeName("Err")
  public static class Err<T, E> extends EdgeResult<T, E> {
    private E content;

    public Err() {
    }

    public Err(E content) {
      super("Err");
      this.content = content;
    }

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

    public EdgeResponseEnum() {
    }

    public EdgeResponseEnum(String kind) {
      this.kind = kind;
    }

    public void setKind(String kind) {
      this.kind = kind;
    }
  }

  @JsonTypeName("SendMessage")
  public static class SendMessageResponse extends EdgeResponseEnum {
    private EdgeResult<WaitAckSuccess, WaitAckError> content;

    public SendMessageResponse() {
    }

    public SendMessageResponse(EdgeResult<WaitAckSuccess, WaitAckError> content) {
      super("SendMessage");
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

    public EndpointOnlineResponse() {
    }

    public EndpointOnlineResponse(String content) {
      super("EndpointOnline");
      this.content = content;
    }

    public String getKind() {
      return "EndpointOnline";
    }

    public String getContent() {
      return content;
    }

    public void setContent(String content) {
      this.content = content;
    }
  }

  @JsonTypeName("EndpointOffline")
  public static class EndpointOfflineResponse extends EdgeResponseEnum {
    public EndpointOfflineResponse() {
      super("EndpointOffline");
    }

    public String getKind() {
      return "EndpointOffline";
    }
  }

  @JsonTypeName("EndpointInterest")
  public static class EndpointInterestResponse extends EdgeResponseEnum {
    public EndpointInterestResponse() {
      super("EndpointInterest");
    }

    public String getKind() {
      return "EndpointInterest";
    }
  }

  @JsonTypeName("SetState")
  public static class SetStateResponse extends EdgeResponseEnum {
    public SetStateResponse() {
      super("SetState");
    }

    public String getKind() {
      return "SetState";
    }
  }

  public static class EdgeResponse {
    private int seqId;
    private EdgeResult<EdgeResponseEnum, EdgeError> result;

    public EdgeResponse() {
    }

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

    public EndpointInterest() {
    }

    public EndpointInterest(String topicCode, String endpoint, List<String> interests) {
      this.topicCode = topicCode;
      this.endpoint = endpoint;
      this.interests = interests;
    }

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

    public MessageHeader() {
    }

    public MessageHeader(String messageId, MessageAckExpectKind ackKind, MessageTargetKind targetKind,
        MessageDurabilityConfig durability, List<String> subjects) {
      this.messageId = messageId;
      this.ackKind = ackKind;
      this.targetKind = targetKind;
      this.durability = durability;
      this.subjects = subjects;
    }

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
    private MessageHeader header;
    private String payload;

    public Message() {
    }

    public Message(MessageHeader header, String payload) {
      this.header = header;
      this.payload = payload;
    }

    public MessageHeader getHeader() {
      return header;
    }

    public void setHeader(MessageHeader header) {
      this.header = header;
    }

    public String getPayload() {
      return payload;
    }

    public void setPayload(String payload) {
      this.payload = payload;
    }
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

    public MessageStateUpdate() {
    }

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

    public SetState() {
    }

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
    private Map<String, MessageStatusKind> status;
    private WaitAckErrorException exception;

    public WaitAckError() {
    }

    public WaitAckError(Map<String, MessageStatusKind> status, WaitAckErrorException exception) {
      this.status = status;
      this.exception = exception;
    }

    public Map<String, MessageStatusKind> getStatus() {
      return status;
    }

    public void setStatus(Map<String, MessageStatusKind> status) {
      this.status = status;
    }

    public WaitAckErrorException getException() {
      return exception;
    }

    public void setException(WaitAckErrorException exception) {
      this.exception = exception;
    }

  }

  public static class WaitAckSuccess {
    private Map<String, MessageStatusKind> status;

    public WaitAckSuccess() {
    }

    public WaitAckSuccess(Map<String, MessageStatusKind> status) {
      this.status = status;
    }

    public Map<String, MessageStatusKind> getStatus() {
      return status;
    }

    public void setStatus(Map<String, MessageStatusKind> status) {
      this.status = status;
    }

  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "kind")
  public static abstract class EdgePayload {
    private String kind;

    public EdgePayload() {
    }

    public EdgePayload(String kind) {
      this.kind = kind;
    }

    public void setKind(String kind) {
      this.kind = kind;
    }
  }

  @JsonTypeName("Push")
  public static class EdgePushPayload extends EdgePayload {
    private EdgePush content;

    public EdgePushPayload() {
    }

    public EdgePushPayload(EdgePush content) {
      super("Push");
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

    public EdgeResponsePayload() {
    }

    public EdgeResponsePayload(EdgeResponse content) {
      super("Response");
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

    public EdgeRequestPayload() {
    }

    public EdgeRequestPayload(EdgeRequest content) {
      super("Request");
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

    public EdgeErrorPayload() {
    }

    public EdgeErrorPayload(EdgeError content) {
      super("Error");
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

    public EdgePush() {
    }

    public EdgePush(String kind) {
      this.kind = kind;
    }

    public void setKind(String kind) {
      this.kind = kind;
    }
  }

  @JsonTypeName("Message")
  public static class MessagePush extends EdgePush {
    private List<String> endpoints;
    private Message message;

    public MessagePush() {
    }

    public MessagePush(List<String> endpoints, Message message) {
      super("Message");
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