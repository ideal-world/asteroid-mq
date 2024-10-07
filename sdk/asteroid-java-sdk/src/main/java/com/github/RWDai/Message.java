package com.github.RWDai;

import java.io.UnsupportedEncodingException;
import java.util.Base64;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.RWDai.Types.EdgeMessage;
import com.github.RWDai.Types.EdgeMessageHeader;
import com.github.RWDai.Types.MessageAckExpectKind;
import com.github.RWDai.Types.MessageDurabilityConfig;
import com.github.RWDai.Types.MessageTargetKind;

public class Message {
  public static class MessageConfig {
    public MessageAckExpectKind ackKind = MessageAckExpectKind.Sent;
    public MessageTargetKind targetKind = MessageTargetKind.Push;
    public MessageDurabilityConfig durability;
    public List<String> subjects;
    public String topic;

  }

  public static EdgeMessageHeader fromConfig(MessageConfig config) {
    return new EdgeMessageHeader(config.ackKind, config.targetKind, config.durability, config.subjects,
        config.topic);
  }

  /**
   * Create a new json message
   * 
   * @param body   the body of the message, will be encoded as json
   * @param config the configuration of the message
   * @return created message
   * @throws JsonProcessingException
   * @throws UnsupportedEncodingException
   */
  public static <T> EdgeMessage newMessage(T body, MessageConfig config)
      throws JsonProcessingException, UnsupportedEncodingException {
    ObjectMapper mapper = new ObjectMapper();
    String json = mapper.writeValueAsString(body);
    byte[] payload = json.getBytes("UTF-8");
    String base64Json = Base64.getEncoder().encodeToString(payload);

    EdgeMessage message = new EdgeMessage(Message.fromConfig(config), base64Json);
    return message;
  }

  /**
   * Create a new text message
   * 
   * @param text   the body of the message, will be encoded as text
   * @param config the configuration of the message
   * @return created message
   */
  public static EdgeMessage newTextMessage(String text, MessageConfig config) {
    byte[] payload = text.getBytes();
    String base64Text = Base64.getEncoder().encodeToString(payload);

    EdgeMessage message = new EdgeMessage(fromConfig(config), base64Text);
    return message;
  }
}
