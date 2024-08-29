# Endpoint(Like an actor)
- addr: unique
- interests: code[]
- description: string
## Endpoint Status
- Busy
- Offline
- Available
## Endpoint Durable


# Single E2E Message
target: 
digest: 
bytes: 

E2E Message Status: 

- Sending
- Received
- Processed
- Deleted

## Fields

### ack
when to delete this message
### duration(optional)
### Target
- Limit (many ep, reached the limit)
- Online (many ep, if interest)
- Available (one ep, if it's available)
- Push (one ep, if it's online)
- Endpoints (certain endpoint)

ACK_TYPE


# Ep to Node Event
## SendMessage
The node will handle this message
## MessageResult

## SetState
Update status
## Ack

# Pattens

## Req/Resp
```
Req:
target: Available(responder.{REQ})
durable: None
ack: Processed
```
```
Resp:
target: Endpoints([{source}])
durable: None
ack: Received
```
## Broadcast with verification
```
Req:
target: Online(event.{SomeEvent})
durable: None
ack: Processed
```

## Durable WorkerQueue
```
Req:
target: Push(event.{SomeEvent})
durable: None
ack: Processed
```


## Edge Node Connections

### Edge 2 Cluster
SendMessage
CreateEdgeEndpoint
DeleteEdgeEndpoint

### Cluster 2 Edge
PushMessage
MessageAck
SendResult



