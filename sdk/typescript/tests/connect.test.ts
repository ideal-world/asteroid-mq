import { sleep, spawn, spawnSync } from "bun";
import { MessageConfig, MessageTargetKind, Node, newMessage } from "../src/index";
import { describe, test } from "bun:test";

const { exitCode: buildExitCode } = spawnSync(["cargo", "build", "-p", "asteroid-mq", "--example", "axum-server"]);
if (buildExitCode !== 0) {
    console.error(`Failed to build server ${buildExitCode}`);
    process.exit(buildExitCode);
}
console.log("waiting for server to start");
const serverProcess = spawn(["cargo", "run", "-p", "asteroid-mq", "--example", "axum-server"], {
    stdout: "inherit",
    stderr: "inherit"
});
await sleep(1000);

interface HelloMessage {
    "hello": string
}
const message = (s: string) => newMessage<HelloMessage>(
    {
        "hello": s
    }, 
    MessageConfig.online("test", ["event/hello", "event/hello/avatar/b2"])
);

describe("basic connection", () => {
    test("basic connection", async () => {
        try {

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

            let sendAsyncTask = nodeA.sendMessage(newMessage("durable", {
                subjects: ["durable/a"],
                topic: "test",
                targetKind: MessageTargetKind.Durable,
                durability: {
                    expire: new Date(Date.now() + 1000),
                    maxReceiver: 1
                }
            }))
            let ep = await nodeB.createEndpoint("test", ["durable/*"]);
            let firstMessage = await ep.messages().next();
            console.log("received message: ", firstMessage.value?.json());
            await sendAsyncTask;
            await nodeA.close();
            await nodeB.close();
            await processTaskB1;
            await processTaskB2;

        } finally {
            serverProcess.kill();
        }

    });
});