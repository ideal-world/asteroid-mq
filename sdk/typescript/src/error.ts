import { EdgeError, EdgeErrorKind, EndpointAddr, MessageStatusKind, WaitAckError } from "./types";

export class EdgeErrorClass extends Error {
    context: string;
    kind: EdgeErrorKind;
    constructor(error: EdgeError) {
        super(error.message);
        this.context = error.context;
        this.kind = error.kind;
    }
}

export class WaitAckErrorClass extends Error {
    status: Record<EndpointAddr, MessageStatusKind>;
    constructor(error: WaitAckError) {
        if (error.exception === null) {
            super("Ack Error");
        } else {
            super(error.exception);
        }
        this.status = error.status;
    }
}