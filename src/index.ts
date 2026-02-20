import net, { Socket } from 'net'
import { exit } from 'process';

let heartbeatInterval: NodeJS.Timeout | null = null;

const main = async () => {
    const socket = net.createConnection({host: "127.0.0.1", port:5673})

    socket.on('connect', () => {
        console.log('Connected to RabbitMQ');
        
        const protocolHeader = Buffer.from([
            0x41, 0x4D, 0x51, 0x50,  // 'A', 'M', 'Q', 'P'
            0x00,                     // Protocol ID (0 for AMQP)
            0x00,                     // Major version (0)
            0x09,                     // Minor version (9)
            0x01                      // Revision (1)
        ])
        
        socket.write(protocolHeader)
        console.log('Sent protocol header');
    });

    let receiveBuffer = Buffer.alloc(0);
    socket.on('readable', () => {
        let data;
        while ((data = socket.read()) !== null) {
            console.log(`Received ${data.length} bytes`);
            
            receiveBuffer = Buffer.concat([receiveBuffer, data]);
            
            while (receiveBuffer.length >= 7) {
                const size = receiveBuffer.readUInt32BE(3);
                const totalFrameSize = 7 + size + 1; // header + payload + end
                
                if (receiveBuffer.length >= totalFrameSize) {
                    const frame = receiveBuffer.subarray(0, totalFrameSize);
                    receiveBuffer = receiveBuffer.subarray(totalFrameSize);
                    
                    handleFrame(frame, socket);
                } else {
                    console.log("Waiting for more data...")
                    break;
                }
            }
        }
    })

    socket.on('error', (err) => {
        console.error("socket err:", err)
    })

    socket.on('close', ()=> {
        console.log("closing connection")
        if (heartbeatInterval) {
            clearInterval(heartbeatInterval);
            heartbeatInterval = null;
        }
    })
}

const handleFrame=(data: Buffer, socket: Socket) => {
    const frameType = data[0];
    const channel = data.readUInt16BE(1)
    const size = data.readUInt32BE(3)
    const payload = data.subarray(7, 7+size)
    const frameEnd = data[7+size]

    if(frameEnd != FRAME_END) {
        console.log("Expecting frame end, got:", frameEnd)
        exit(1)
    }
    if (frameType === 8) {
        console.log("Heartbeat received from server");
        return;
    }

    // Handle method frames
    if (frameType === 1) {
        const classId = payload.readUInt16BE(0)
        const methodId = payload.readUInt16BE(2)
        console.log("classId, methodId:", classId, methodId)
        
        if (classId === 10 && methodId === 10) {
            // Connection.Start
            handleConnStartFrame(payload)
            sendConnectionStartOK(socket)
        } else if (classId === 10 && methodId === 30) {
            // Connection.Tune
            console.log("Connection.Tune received");
            handleConnectionTune(payload, socket);
        } else if (classId === 10 && methodId === 41) {
            // Connection.Open-OK
            console.log("Connection.Open-OK received! Connection established!");
        } else {
            console.log("Unknown method:", classId, methodId);
        }
    }
} 

const FRAME_END = 0xCE; // 206

const handleConnStartFrame=(payload: Buffer) => {
    console.log("connection.start received");
    let offset = 4;
    
    const versionMajor = payload.readUInt8(offset);
    offset += 1;
    
    const versionMinor = payload.readUInt8(offset);
    offset += 1;
    
    console.log(`AMQP version: ${versionMajor}.${versionMinor}`);
    
    const serverPropsLength = payload.readUInt32BE(offset);
    offset += 4;
    const serverPropsData = payload.slice(offset, offset + serverPropsLength);
    offset += serverPropsLength;
    
    const serverProps = parseFieldTable(serverPropsData);
    console.log("Server properties:", serverProps);
    
    const mechLen = payload.readUInt32BE(offset);
    offset += 4;
    const mechanisms = payload.toString('utf8', offset, offset + mechLen);
    offset += mechLen;
    console.log("Mechanisms:", mechanisms);
    
    const localesLen = payload.readUInt32BE(offset);
    offset += 4;
    const locales = payload.toString('utf8', offset, offset + localesLen);
    console.log("Locales:", locales);
} 

function handleConnectionTune(payload: Buffer, socket: Socket) {
    let offset = 4;

    const channelMax = payload.readUInt16BE(offset);
    offset += 2;
    console.log("Channel max:", channelMax);

    const frameMax = payload.readUInt32BE(offset);
    offset += 4;
    console.log("Frame max:", frameMax);

    const heartbeat = payload.readUInt16BE(offset);
    offset += 2;
    console.log("Heartbeat:", heartbeat);

    sendConnectionTuneOK(socket, channelMax, frameMax, heartbeat);
    
    sendConnectionOpen(socket);

    if (heartbeat > 0) {
        startHeartbeat(socket, heartbeat);
    }
}

function startHeartbeat(socket: Socket, intervalSeconds: number) {
    console.log(`Starting heartbeat every ${intervalSeconds} seconds`);
    
    heartbeatInterval = setInterval(() => {
        sendHeartbeat(socket);
    }, intervalSeconds * 1000);
}

function sendHeartbeat(socket: Socket) {
    // Heartbeat frame: type=8, channel=0, size=0, no payload, frame-end
    const frame = Buffer.alloc(8);
    frame.writeUInt8(8, 0);           // Frame type: Heartbeat
    frame.writeUInt16BE(0, 1);        // Channel: 0
    frame.writeUInt32BE(0, 3);        // Size: 0 (no payload)
    frame.writeUInt8(0xCE, 7);        // Frame end
    
    socket.write(frame);
    console.log('Sent heartbeat');
}

function sendConnectionTuneOK(socket: Socket, channelMax: number, frameMax: number, heartbeat: number) {
    const classId = Buffer.alloc(2);
    classId.writeUInt16BE(10, 0);

    const methodId = Buffer.alloc(2);
    methodId.writeUInt16BE(31, 0); // Tune-OK = 31

    const channelMaxBuf = Buffer.alloc(2);
    channelMaxBuf.writeUInt16BE(channelMax, 0);

    const frameMaxBuf = Buffer.alloc(4);
    frameMaxBuf.writeUInt32BE(frameMax, 0);

    const heartbeatBuf = Buffer.alloc(2);
    heartbeatBuf.writeUInt16BE(heartbeat, 0);

    const fullPayload = Buffer.concat([
        classId,
        methodId,
        channelMaxBuf,
        frameMaxBuf,
        heartbeatBuf
    ]);

    const frame = Buffer.alloc(7 + fullPayload.length + 1);
    frame.writeUInt8(1, 0);
    frame.writeUInt16BE(0, 1);
    frame.writeUInt32BE(fullPayload.length, 3);
    fullPayload.copy(frame, 7);
    frame.writeUInt8(0xCE, frame.length - 1);

    socket.write(frame);
    console.log('Sent Connection.Tune-OK');
}

function sendConnectionOpen(socket: Socket) {
    const classId = Buffer.alloc(2);
    classId.writeUInt16BE(10, 0);

    const methodId = Buffer.alloc(2);
    methodId.writeUInt16BE(40, 0); // Open = 40

    const vhost = Buffer.from('/');
    const vhostEncoded = Buffer.concat([
        Buffer.from([vhost.length]),
        vhost
    ]);

    const reserved1 = Buffer.from([0]); // Empty shortstr
    const reserved2 = Buffer.from([0]); // bit field (1 byte)

    const fullPayload = Buffer.concat([
        classId,
        methodId,
        vhostEncoded,
        reserved1,
        reserved2
    ]);

    const frame = Buffer.alloc(7 + fullPayload.length + 1);
    frame.writeUInt8(1, 0);
    frame.writeUInt16BE(0, 1);
    frame.writeUInt32BE(fullPayload.length, 3);
    fullPayload.copy(frame, 7);
    frame.writeUInt8(0xCE, frame.length - 1);

    socket.write(frame);
    console.log('Sent Connection.Open');
}

function parseFieldTable(buffer: Buffer): Record<string, any> {
    const result: Record<string, any> = {};
    let offset = 0;
    
    while (offset < buffer.length) {
        const keyLen = buffer.readUInt8(offset);
        offset += 1;
        const key = buffer.toString('utf8', offset, offset + keyLen);
        offset += keyLen;
        
        const valueType = String.fromCharCode(buffer.readUInt8(offset));
        offset += 1;
        
        switch (valueType) {
            case 't': // boolean
                result[key] = buffer.readUInt8(offset) !== 0;
                offset += 1;
                break;
            case 'b': // signed 8-bit
                result[key] = buffer.readInt8(offset);
                offset += 1;
                break;
            case 's': // signed 16-bit
                result[key] = buffer.readInt16BE(offset);
                offset += 2;
                break;
            case 'I': // signed 32-bit
                result[key] = buffer.readInt32BE(offset);
                offset += 4;
                break;
            case 'l': // signed 64-bit (use BigInt)
                result[key] = buffer.readBigInt64BE(offset);
                offset += 8;
                break;
            case 'S': // longstr
                const strLen = buffer.readUInt32BE(offset);
                offset += 4;
                result[key] = buffer.toString('utf8', offset, offset + strLen);
                offset += strLen;
                break;
            case 'F': // nested field table
                const tableLen = buffer.readUInt32BE(offset);
                offset += 4;
                result[key] = parseFieldTable(buffer.slice(offset, offset + tableLen));
                offset += tableLen;
                break;
            default:
                console.log("valueType doesn't exist", valueType)
                exit(1)
        }
    }
    
    return result;
}

function sendConnectionStartOK(socket: net.Socket) {
    const classId = Buffer.alloc(2);
    classId.writeUInt16BE(10, 0);
    
    const methodId = Buffer.alloc(2);
    methodId.writeUInt16BE(11, 0);
    
    const clientProps = encodeFieldTable({
        'product': 'my-amqp-client',
        'version': '0.1.0',
        'platform': 'Node.js',
        'capabilities': {
            'authentication_failure_close': true,
            'publisher_confirms': true,
            'consumer_cancel_notify': true
        }
    });
    
    const mechanism = Buffer.from('PLAIN');
    const mechanismEncoded = Buffer.concat([
        Buffer.from([mechanism.length]),
        mechanism
    ]);
    
    const username = 'guest';
    const password = 'guest';
    const saslResponse = Buffer.from(`\0${username}\0${password}`, 'utf8');
    const responseEncoded = Buffer.alloc(4 + saslResponse.length);
    responseEncoded.writeUInt32BE(saslResponse.length, 0);
    saslResponse.copy(responseEncoded, 4);
    
    const locale = Buffer.from('en_US');
    const localeEncoded = Buffer.concat([
        Buffer.from([locale.length]),
        locale
    ]);
    
    const fullPayload = Buffer.concat([
        classId,
        methodId,
        clientProps,
        mechanismEncoded,
        responseEncoded,
        localeEncoded
    ]);
    
    const frame = Buffer.alloc(7 + fullPayload.length + 1);
    frame.writeUInt8(1, 0);
    frame.writeUInt16BE(0, 1);
    frame.writeUInt32BE(fullPayload.length, 3);
    fullPayload.copy(frame, 7);
    frame.writeUInt8(0xCE, frame.length - 1);
    
    socket.write(frame);
    console.log('Sent Connection.Start-OK');
}

function encodeFieldTable(obj: Record<string, any>): Buffer {
    const parts: Buffer[] = [];
    
    for (const [key, value] of Object.entries(obj)) {
        const keyBuf = Buffer.from(key);
        parts.push(Buffer.from([keyBuf.length]));
        parts.push(keyBuf);
        
        if (typeof value === 'string') {
            parts.push(Buffer.from('S'));
            const valueBuf = Buffer.from(value);
            const lenBuf = Buffer.alloc(4);
            lenBuf.writeUInt32BE(valueBuf.length, 0);
            parts.push(lenBuf);
            parts.push(valueBuf);
        } else if (typeof value === 'boolean') {
            parts.push(Buffer.from('t'));
            parts.push(Buffer.from([value ? 1 : 0]));
        } else if (typeof value === 'object') {
            parts.push(Buffer.from('F'));
            const nested = encodeFieldTableContent(value);
            const lenBuf = Buffer.alloc(4);
            lenBuf.writeUInt32BE(nested.length, 0);
            parts.push(lenBuf);
            parts.push(nested);
        }
    }
    
    const tableBuf = Buffer.concat(parts);
    const result = Buffer.alloc(4 + tableBuf.length);
    result.writeUInt32BE(tableBuf.length, 0);
    tableBuf.copy(result, 4);
    
    return result;
}

function encodeFieldTableContent(obj: Record<string, any>): Buffer {
    const parts: Buffer[] = [];
    
    for (const [key, value] of Object.entries(obj)) {
        const keyBuf = Buffer.from(key);
        parts.push(Buffer.from([keyBuf.length]));
        parts.push(keyBuf);
        
        if (typeof value === 'string') {
            parts.push(Buffer.from('S'));
            const valueBuf = Buffer.from(value);
            const lenBuf = Buffer.alloc(4);
            lenBuf.writeUInt32BE(valueBuf.length, 0);
            parts.push(lenBuf);
            parts.push(valueBuf);
        } else if (typeof value === 'boolean') {
            parts.push(Buffer.from('t'));
            parts.push(Buffer.from([value ? 1 : 0]));
        } else if (typeof value === 'object') {
            parts.push(Buffer.from('F'));
            const nested = encodeFieldTableContent(value);
            const lenBuf = Buffer.alloc(4);
            lenBuf.writeUInt32BE(nested.length, 0);
            parts.push(lenBuf);
            parts.push(nested);
        }
    }
    
    return Buffer.concat(parts);
}

main()
