package com.github.RWDai;

import java.util.concurrent.CompletableFuture;

import com.github.RWDai.Types.MessageHeader;

public interface ReceivedMessage {
  /**
   * Get the header of the message
   */
  MessageHeader getHeader();

  /**
   * Get the payload of the message in binary
   */
  byte[] getPayload();

  /**
   * Ack the message as received
   */
  CompletableFuture<Void> received();

  /**
   * Ack the message as processed
   */
  CompletableFuture<Void> processed();

  /**
   * Ack the message as failed
   */
  CompletableFuture<Void> failed();

  /**
   * Decode the payload as json
   */
  <T> T json(Class<T> clazz);

  /**
   * Decode the payload as text
   */
  String text();

  /**
   * Get the endpoint
   */
  Endpoint getEndpoint();
}
