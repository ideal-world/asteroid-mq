import { MessageTargetKind, Node, newMessage } from "../src";
interface HelloMessage {
    "hello": string
}
const message = (s: string) => newMessage<HelloMessage>(
    {
        "hello": s
    }, {
    topic: "test",
    subjects: ["event/hello", "event/hello/avatar/b2"],
    targetKind: MessageTargetKind.Online
});

const nodeIdA = await (await fetch("http://localhost:8080/node_id", {
    "method": "PUT"
})).text();
const nodeIdB = await (await fetch("http://localhost:8080/node_id", {
    "method": "PUT"
})).text();

const nodeA = Node.connect({
    url: `ws://localhost:8080/connect?node_id=${nodeIdA}`
})
const nodeB = Node.connect({
    url: `ws://localhost:8080/connect?node_id=${nodeIdB}`
})

const endpointB1 = await nodeB.createEndpoint("test", ["event/*"]);
const endpointB2 = await nodeB.createEndpoint("test", ["event/**/b2"]);
console.log(endpointB1.address);
endpointB1.updateInterests(["event/hello"]);
const processTaskB1 = new Promise((resolve) => {
    (
        async () => {
            for await (const message of endpointB1.messages()) {
                if (message !== undefined) {
                    const payload = message.json<HelloMessage>();
                    message.received();
                    console.log(payload);
                    message.processed();
                } else {
                    resolve(undefined);
                }
            }
        }
    )();
})
const processTaskB2 = new Promise((resolve) => {
    (
        async () => {
            for await (const message of endpointB2.messages()) {
                if (message !== undefined) {
                    const payload = message.json<HelloMessage>();
                    message.received();
                    console.log(payload);
                    message.processed();
                } else {
                    resolve(undefined);
                }
            }
        }
    )();
})
await nodeA.sendMessage(message("world"));
await nodeA.sendMessage(message("alice"));
await nodeA.sendMessage(message("bob"));
await nodeA.close();
await nodeB.close();
await processTaskB1;
await processTaskB2;