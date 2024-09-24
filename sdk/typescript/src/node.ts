import { Endpoint } from "./endpoint";
import { EdgeErrorClass, WaitAckErrorClass } from "./error";
import { ReceivedMessage } from "./message";
import { EdgeError, EdgeMessage, EdgePayload, EdgeRequest, EdgeResponseEnum, EdgeResult, EndpointAddr, Interest, MessageStatusKind, TopicCode } from "./types";


export class Node {
    private socket: WebSocket;
    private requestId = 0;
    private responseWaitingPool = new Map<number, {
        resolve(result: EdgeResult<EdgeResponseEnum, EdgeError>): void;
        reject(error: any): void;
    }>();
    private openingWaitingPool = new Set<{
        resolve(): void;
        reject(error: any): void;
    }>();
    private alive = false;
    private endpoints = new Map<EndpointAddr, Endpoint>;
    private textDecoder = new TextDecoder();

    /**
     * Create a new Node by connecting to the given URL
     * @param options options for the connection
     * @returns a new Node instance
     */
    static connect(options: {
        url: string | URL;
    }): Node {
        const socket = new WebSocket(options.url);
        socket.binaryType = "arraybuffer";
        const node = new Node(socket);
        socket.onmessage = (evt) => {
            if (evt.data instanceof ArrayBuffer) {
                let text = new TextDecoder().decode(evt.data);
                let payload: EdgePayload = JSON.parse(text);

                switch (payload.kind) {
                    case "Response":
                        {
                            const result = payload.content.result;
                            const seqId = payload.content.seq_id;
                            const channel = node.responseWaitingPool.get(seqId);
                            channel?.resolve(result);
                        }
                        break;
                    case "Push":
                        {
                            const content = payload.content;
                            switch (content.kind) {
                                case "Message":
                                    {
                                        const { message, endpoints } = content.content;
                                        for (const ep of endpoints) {
                                            const endpoint = node.endpoints.get(ep);
                                            if (endpoint === undefined) {
                                                continue;
                                            }
                                            let receivedMessage: ReceivedMessage = {
                                                header: message.header,
                                                payload: new Uint8Array(Buffer.from(atob(message.payload))),
                                                received: async () => {
                                                    await node.ackMessage(endpoint, message.header.message_id, MessageStatusKind.Received);
                                                },
                                                processed: async () => {
                                                    await node.ackMessage(endpoint, message.header.message_id, MessageStatusKind.Processed);
                                                },
                                                failed: async () => {
                                                    await node.ackMessage(endpoint, message.header.message_id, MessageStatusKind.Failed);
                                                },
                                                json: () => {
                                                    return JSON.parse(node.textDecoder.decode(receivedMessage.payload));
                                                },
                                                text: () => {
                                                    return node.textDecoder.decode(receivedMessage.payload);
                                                },
                                                endpoint
                                            }
                                            endpoint.receive(receivedMessage);
                                        }

                                    }
                                    break;
                            }
                        }
                    case "Request":
                    case "Error":
                }
            }
        }
        socket.onopen = (_evt) => {
            node.alive = true;
            node.openingWaitingPool.forEach((channel) => {
                channel.resolve();
            })
        }
        socket.onclose = (_evt) => {
            node.alive = false;
        }
        return node;
    }
    /**
     * create a new Node instance by a given websocket connection
     * 
     * It is **recommended** to use `Node.connect` to create a new Node instance
     * @param socket the underlying websocket connection
     */
    constructor(socket: WebSocket) {
        this.socket = socket;
    }
    private sendPayload(payload: EdgePayload) {
        let text = JSON.stringify(payload);
        let binary = new TextEncoder().encode(text);
        this.socket.send(binary);
    }
    private nextRequestId() {
        return ++this.requestId;
    }
    private waitResponse(requestId: number): Promise<EdgeResult<EdgeResponseEnum, EdgeError>> {
        return new Promise<EdgeResult<EdgeResponseEnum, EdgeError>>((respResolve, respReject) => {
            this.responseWaitingPool.set(requestId, {
                resolve: respResolve,
                reject: respReject
            })
        })
    }
    private waitSocketOpen() {
        return new Promise<void>((resolve, reject) => {
            if (this.alive) {
                resolve();
            } else {
                this.openingWaitingPool.add({
                    resolve,
                    reject
                })
            }
        })
    }
    /**
     * create a new endpoint
     * @param topic topic code
     * @param interests interests
     * @returns a new endpoint
     */
    public async createEndpoint(topic: TopicCode, interests: Interest[]): Promise<Endpoint> {
        await this.waitSocketOpen();
        const requestId = this.nextRequestId();
        const request: EdgeRequest = {
            seq_id: requestId,
            request: {
                kind: "EndpointOnline",
                content: {
                    topic_code: topic,
                    interests,
                }
            }
        };
        const waitResponse = this.waitResponse(requestId);
        this.sendPayload({
            "kind": "Request",
            "content": request
        })
        const response = await waitResponse;
        if (response.kind !== "Ok") {
            throw new EdgeErrorClass(response.content);
        }
        if (response.content.kind !== "EndpointOnline") {
            throw new Error(`Unexpected response kind ${response.content.kind}`);
        }
        const addr = response.content.content;
        const endpoint = new Endpoint(this, {
            topic,
            address: addr,
            interest: new Set(interests)
        });
        this.endpoints.set(addr, endpoint);
        return endpoint
    }
    public async destroyEndpoint(endpoint: Endpoint): Promise<void> {
        if (!this.alive) {
            return;
        }
        endpoint.closeMessageChannel();
        const requestId = this.nextRequestId();
        const request: EdgeRequest = {
            seq_id: requestId,
            request: {
                kind: "EndpointOffline",
                content: {
                    endpoint: endpoint.address,
                    topic_code: endpoint.topic
                }
            }
        };
        const waitResponse = this.waitResponse(requestId);
        this.sendPayload({
            "kind": "Request",
            "content": request
        })
        const response = await waitResponse;
        if (response.kind !== "Ok") {
            throw new EdgeErrorClass(response.content);
        }
        if (response.content.kind !== "EndpointOffline") {
            throw new Error(`Unexpected response kind ${response.content.kind}`);
        }
        this.endpoints.delete(endpoint.address);
    }
    public async updateInterest(endpoint: Endpoint, interests: Interest[]): Promise<void> {
        if (!this.alive) {
            return;
        }
        const requestId = this.nextRequestId();
        const request: EdgeRequest = {
            seq_id: requestId,
            request: {
                kind: "EndpointInterest",
                content: {
                    endpoint: endpoint.address,
                    topic_code: endpoint.topic,
                    interests
                }
            }
        };
        const waitResponse = this.waitResponse(requestId);
        this.sendPayload({
            "kind": "Request",
            "content": request
        })
        const response = await waitResponse;
        if (response.kind !== "Ok") {
            throw new EdgeErrorClass(response.content);
        }
        if (response.content.kind !== "EndpointInterest") {
            throw new Error(`Unexpected response kind ${response.content.kind}`);
        }
    }
    public async ackMessage(endpoint: Endpoint, messageId: string, state: MessageStatusKind): Promise<void> {
        await this.waitSocketOpen();
        let requestId = this.nextRequestId();
        let request: EdgeRequest = {
            seq_id: requestId,
            request: {
                kind: "SetState",
                content: {
                    topic: endpoint.topic,
                    update: {
                        message_id: messageId,
                        status: {
                            [endpoint.address]: state
                        }
                    }
                }
            }
        };
        let waitResponse = this.waitResponse(requestId);
        this.sendPayload({
            "kind": "Request",
            "content": request
        })
        let response = await waitResponse;
        if (response.kind !== "Ok") {
            throw new EdgeErrorClass(response.content);
        }
        if (response.content.kind !== "SetState") {
            throw new Error(`Unexpected response kind ${response.content.kind}`);
        }
    }
    public async sendMessage(message: EdgeMessage): Promise<void> {
        await this.waitSocketOpen();
        let requestId = this.nextRequestId();
        let request: EdgeRequest = {
            seq_id: requestId,
            request: {
                kind: "SendMessage",
                content: message
            }
        };
        let waitResponse = this.waitResponse(requestId);
        this.sendPayload({
            "kind": "Request",
            "content": request
        })
        let response = await waitResponse;
        if (response.kind !== "Ok") {
            throw new EdgeErrorClass(response.content);
        }
        if (response.content.kind !== "SendMessage") {
            throw new Error(`Unexpected response kind ${response.content.kind}`);
        }
        let ackResult = response.content.content;
        if (ackResult.kind !== "Ok") {
            console.error(ackResult);
            throw new WaitAckErrorClass(ackResult.content)
        }
    }
    public isAlive(): boolean {
        return this.alive;
    }
    public async close() {
        await Promise.all(Array.from(this.endpoints.values()).map((endpoint) => {
            return this.destroyEndpoint(endpoint);
        }))
        this.socket.close();
    }
}
