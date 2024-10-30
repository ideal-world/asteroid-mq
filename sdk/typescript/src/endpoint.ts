import { EndpointAddr, Interest, Message, MessageStatusKind, TopicCode } from "./types";
import { Node } from './node';
import { ReceivedMessage } from "./message";
export class Endpoint {
    readonly node: Node;
    readonly topic: TopicCode;
    private interests: Set<Interest>;
    readonly address: EndpointAddr;
    readonly isOnline: boolean = false;
    private messageQueue: ReceivedMessage[] = [];
    private waitingNextMessage?: {
        resolve: (message: ReceivedMessage | undefined) => void;
    };
    constructor(node: Node, config: {
        topic: TopicCode;
        interest: Set<Interest>;
        address: EndpointAddr;
    }) {
        this.node = node;
        this.topic = config.topic;
        this.address = config.address;
        this.interests = config.interest;
    }
    public async offline() {
        await this.node.destroyEndpoint(this);
    }
    public getInterest(): Interest[] {
        return Array.from(this.interests);
    }
    public async modifyInterests(modify: (interests: Set<Interest>) => Set<Interest>) {
        const newInterests = modify(new Set(this.interests));
        await this.node.updateInterests(this, Array.from(newInterests));
    }
    public async updateInterests(interests: Interest[]) {
        await this.node.updateInterests(this, interests);
        this.interests = new Set(interests);
    }
    public receive(message: Message) {
        let receivedMessage: ReceivedMessage = {
            header: message.header,
            payload: new Uint8Array(Buffer.from(atob(message.payload))),
            received: async () => {
                await this.node.ackMessage(this, message.header.message_id, MessageStatusKind.Received);
            },
            processed: async () => {
                await this.node.ackMessage(this, message.header.message_id, MessageStatusKind.Processed);
            },
            failed: async () => {
                await this.node.ackMessage(this, message.header.message_id, MessageStatusKind.Failed);
            },
            json: () => {
                return JSON.parse(this.node.textDecoder.decode(receivedMessage.payload));
            },
            text: () => {
                return this.node.textDecoder.decode(receivedMessage.payload);
            },
            endpoint: this
        }
        if (this.waitingNextMessage !== undefined) {
            this.waitingNextMessage.resolve(receivedMessage);
        } else {
            this.messageQueue.push(receivedMessage);
        }
    }
    public closeMessageChannel() {
        this.waitingNextMessage?.resolve(undefined);
    }
    public async ackReceived(message: Message) {
        await this.node.ackMessage(this, message.header.message_id, MessageStatusKind.Received);
    }
    public async ackProcessed(message: Message) {
        await this.node.ackMessage(this, message.header.message_id, MessageStatusKind.Processed);
    }
    public async ackFailed(message: Message) {
        await this.node.ackMessage(this, message.header.message_id, MessageStatusKind.Failed);
    }
    public async *messages() {
        while (this.node.isAlive()) {
            if (this.messageQueue.length > 0) {
                yield this.messageQueue.shift();
            } else {
                yield await new Promise<ReceivedMessage | undefined>((resolve) => {
                    this.waitingNextMessage = {
                        resolve
                    };
                })
            }
        }
    }
}