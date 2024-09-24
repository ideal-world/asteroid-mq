import { EndpointAddr, Interest, Message, MessageStatusKind, TopicCode } from "./types";
import { Node } from './node';
import { ReceivedMessage } from "./message";
export class Endpoint {
    readonly node: Node;
    readonly topic: TopicCode;
    private interest: Set<Interest>;
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
        this.interest = config.interest;
    }
    public async offline() {
        await this.node.destroyEndpoint(this);
    }
    public getInterest(): Interest[] {
        return Array.from(this.interest);
    }
    public async updateInterest(interests: Interest[]) {
        this.interest = new Set(interests);
        await this.node.updateInterest(this, interests);
    }
    public receive(message: ReceivedMessage) {
        if (this.waitingNextMessage !== undefined) {
            this.waitingNextMessage.resolve(message);
        } else {
            this.messageQueue.push(message);
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