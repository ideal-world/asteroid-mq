import { Endpoint } from "./endpoint";
import { EdgeMessage, EdgeMessageHeader, MessageAckExpectKind, MessageHeader, MessageTargetKind, Subject, TopicCode } from "./types";

export type MessageConfig = {
    /**
     * The kind of ack expected, default to `MessageAckExpectKind.Sent`
     */
    ackKind?: MessageAckExpectKind,
    /** 
     * The target kind of the message, default to `MessageTargetKind.Push`
     */
    targetKind?: MessageTargetKind,
    /**
     * The durability configuration of the message, default to `undefined`
     */
    durability?: {
        expire: Date,
        maxReceiver?: number,
    },
    /**
     * The subjects of the message, at least one subject is required
     */
    subjects: Exclude<Subject[], []>,
    /**
     * The topic of the message
     */
    topic: TopicCode,
}


export namespace MessageConfig {
    /*
    * The default expire time of the message: 1 day
    */
    const DEFAULT_EXPIRE = 1000 * 60 * 60 * 24;
    export function push(topic: TopicCode, subjects: Subject[], ): MessageConfig {
        return {
            subjects,
            topic,
            targetKind: MessageTargetKind.Push
        }
    }
    export function pull(topic: TopicCode, subjects: Subject[], ): MessageConfig {
        return {
            subjects,
            topic,
            targetKind: MessageTargetKind.Durable,
            durability: {
                expire: new Date(Date.now() + DEFAULT_EXPIRE),
                maxReceiver: 1
            }
        }
    }
    export function online(topic: TopicCode, subjects: Subject[], ): MessageConfig {
        return {
            subjects,
            topic,
            targetKind: MessageTargetKind.Online
        }
    }
    export function durable(topic: TopicCode, subjects: Subject[], expire?: Date, maxReceiver?: number): MessageConfig {
        return {
            subjects,
            topic,
            targetKind: MessageTargetKind.Durable,
            durability: {
                expire: expire ?? new Date(Date.now() + DEFAULT_EXPIRE),
                maxReceiver
            }
        }
    }
}
/**
 * The message received by the endpoint
 */
export interface ReceivedMessage {
    /**
     * the header of the message
     */
    header: MessageHeader;
    /**
     * the payload of the message in binary
     */
    payload: Uint8Array;
    /**
     * Ack the message as received
     */
    received(): Promise<void>;
    /**
     * Ack the message as processed
     */
    processed(): Promise<void>;
    /**
     * Ack the message as failed
     */
    failed(): Promise<void>;
    /**
     * Decode the payload as json
     */
    json<T>(): T;
    /**
     * Decode the payload as text
     */
    text(): string;
    /**
     * Decode the payload as binary
     */
    endpoint: Endpoint;
}

// convert the config to the header
function fromConfig(config: MessageConfig): EdgeMessageHeader {
    const ack_kind = config.ackKind ?? MessageAckExpectKind.Sent;
    const target_kind = config.targetKind ?? MessageTargetKind.Push;
    const durability = config.durability ? {
        expire: config.durability.expire,
        max_receiver: config.durability.maxReceiver
    } : undefined;
    return {
        ack_kind,
        target_kind,
        durability,
        subjects: config.subjects,
        topic: config.topic
    }
}

/**
 * Create a new json message
 * @param body the body of the message, will be encoded as json
 * @param config the configuration of the message
 * @returns created message
 */
export function newMessage<T>(body: T, config: MessageConfig): EdgeMessage {
    const json = JSON.stringify(body);
    const payload = new TextEncoder().encode(json);
    const base64Json = Buffer.from(payload).toString('base64');
    return {
        header: fromConfig(config),
        payload: base64Json
    }
}

/**
 * Create a new text message
 * @param text the text of the message
 * @param config the configuration of the message
 * @returns created message
 */
export function newTextMessage(text: string, config: MessageConfig): EdgeMessage {
    const payload = new TextEncoder().encode(text);
    const base64Json = Buffer.from(payload).toString('base64');
    return {
        header: fromConfig(config),
        payload: base64Json
    }
}   