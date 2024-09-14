
typealias EndpointAddr = String

/// # Interest
/// ## Glob Match Interest
/// (/)?(<path>|<*>|<**>)/*
typealias Interest = String

typealias MessageId = String

typealias Subject = String

/// code are expect to be a valid utf8 string
typealias TopicCode = Bytes

@Serializable
enum class EdgeErrorKind(val string: String) {
	@SerialName("Decode")
	Decode("Decode"),
	@SerialName("TopicNotFound")
	TopicNotFound("TopicNotFound"),
	@SerialName("Internal")
	Internal("Internal"),
}

@Serializable
data class EdgeError (
	val context: String,
	val message: String? = null,
	val kind: EdgeErrorKind
)

@Serializable
enum class MessageAckExpectKind(val string: String) {
	@SerialName("Sent")
	Sent("Sent"),
	@SerialName("Received")
	Received("Received"),
	@SerialName("Processed")
	Processed("Processed"),
}

@Serializable
enum class MessageTargetKind(val string: String) {
	@SerialName("Durable")
	Durable("Durable"),
	@SerialName("Online")
	Online("Online"),
	@SerialName("Available")
	Available("Available"),
	@SerialName("Push")
	Push("Push"),
}

@Serializable
data class MessageDurabilityConfig (
	val expire: DateTime<Utc>,
	val max_receiver: UInt? = null
)

@Serializable
data class EdgeMessageHeader (
	val ack_kind: MessageAckExpectKind,
	val target_kind: MessageTargetKind,
	val durability: MessageDurabilityConfig? = null,
	val subjects: List<Subject>,
	val topic: TopicCode
)

@Serializable
data class EdgeMessage (
	val header: EdgeMessageHeader,
	val payload: Bytes
)

@Serializable
sealed class EdgeRequestEnum {
	@Serializable
	@SerialName("SendMessage")
	data class SendMessage(val content: EdgeMessage): EdgeRequestEnum()
}

@Serializable
data class EdgeRequest (
	val seq_id: UInt,
	val request: EdgeRequestEnum
)

@Serializable
sealed class EdgeResult<T, E> {
	@Serializable
	@SerialName("Ok")
	data class Ok<T, E>(val content: T): EdgeResult<T, E>()
	@Serializable
	@SerialName("Err")
	data class Err<T, E>(val content: E): EdgeResult<T, E>()
}

@Serializable
sealed class EdgeResponseEnum {
	@Serializable
	@SerialName("SendMessage")
	data class SendMessage(val content: EdgeResult<WaitAckSuccess, WaitAckError>): EdgeResponseEnum()
}

@Serializable
data class EdgeResponse (
	val seq_id: UInt,
	val result: EdgeResult<EdgeResponseEnum, EdgeError>
)

@Serializable
data class MessageHeader (
	val message_id: MessageId,
	val ack_kind: MessageAckExpectKind,
	val target_kind: MessageTargetKind,
	val durability: MessageDurabilityConfig? = null,
	val subjects: List<Subject>
)

@Serializable
data class Message (
	val header: MessageHeader,
	val payload: Bytes
)

@Serializable
enum class MessageStatusKind(val string: String) {
	@SerialName("Sending")
	Sending("Sending"),
	@SerialName("Unsent")
	Unsent("Unsent"),
	@SerialName("Sent")
	Sent("Sent"),
	@SerialName("Received")
	Received("Received"),
	@SerialName("Processed")
	Processed("Processed"),
	@SerialName("Failed")
	Failed("Failed"),
	@SerialName("Unreachable")
	Unreachable("Unreachable"),
}

@Serializable
enum class WaitAckErrorException(val string: String) {
	@SerialName("MessageDropped")
	MessageDropped("MessageDropped"),
	@SerialName("Overflow")
	Overflow("Overflow"),
	@SerialName("NoAvailableTarget")
	NoAvailableTarget("NoAvailableTarget"),
}

@Serializable
data class WaitAckError (
	val status: HashMap<EndpointAddr, MessageStatusKind>,
	val exception: WaitAckErrorException? = null
)

@Serializable
data class WaitAckSuccess (
	val status: HashMap<EndpointAddr, MessageStatusKind>
)

@Serializable
sealed class EdgePayload {
	@Serializable
	@SerialName("Push")
	data class Push(val content: EdgePush): EdgePayload()
	@Serializable
	@SerialName("Response")
	data class Response(val content: EdgeResponse): EdgePayload()
	@Serializable
	@SerialName("Request")
	data class Request(val content: EdgeRequest): EdgePayload()
	@Serializable
	@SerialName("Error")
	data class Error(val content: EdgeError): EdgePayload()
}

@Serializable
sealed class EdgePush {
	@Serializable
	@SerialName("Message")
	data class Message(val content: Message): EdgePush()
}

