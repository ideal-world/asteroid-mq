use asteroid_mq::protocol::{
    codec::CodecType,
    endpoint::{DelegateMessage, Message, MessageAckExpectKind, MessageHeader},
    interest::Subject,
    topic::TopicCode,
};

#[test]
fn test_codec() {
    let delegate_message = DelegateMessage {
        message: Message::new(
            MessageHeader::builder([Subject::new("events/hello-world")])
                .ack_kind(MessageAckExpectKind::Processed)
                .mode_online()
                .build(),
            "hello-message",
        ),
        topic: TopicCode::const_new("events/hello"),
    };
    let bytes = delegate_message.encode_to_bytes();
    let raw_message = DelegateMessage::decode_from_bytes(bytes).unwrap();
    assert_eq!(delegate_message.topic, raw_message.topic);
}
