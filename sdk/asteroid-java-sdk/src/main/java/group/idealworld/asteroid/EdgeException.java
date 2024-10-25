package group.idealworld.asteroid;

public class EdgeException {

  public static class NodeException extends RuntimeException {
    public NodeException(String message) {
      super(message);
    }

    public NodeException(String string, Exception e) {
      super(string, e);
    }
  }

  public static class EndpointException extends RuntimeException {
    public EndpointException(Exception e) {
      super(e);
    }
  }

  /// Unknown response type
  public static class UnknownResponseException extends RuntimeException {
    public UnknownResponseException(String message) {
      super(message);
    }
  }
}
