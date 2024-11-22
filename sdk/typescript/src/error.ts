import { EdgeError, EdgeErrorKind, EndpointAddr, MessageStatusKind, WaitAckError, WaitAckErrorException } from "./types";

export class EdgeErrorClass extends Error {
    readonly context: string;
    readonly kind: EdgeErrorKind;
    constructor(error: EdgeError) {
        super(error.message);
        this.context = error.context;
        this.kind = error.kind;
    }
}

export class WaitAckErrorClass extends Error {
    readonly status: Record<EndpointAddr, MessageStatusKind>;
    readonly exception?: WaitAckErrorException;
    constructor(error: WaitAckError) {
        if (error.exception === null) {
            super("Ack Error");
        } else {
            super(error.exception);
        }
        this.status = error.status;
        this.exception = error.exception;
    }
}