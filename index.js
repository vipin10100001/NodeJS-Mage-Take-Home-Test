const net = require('net');
const fs = require('fs');

const HOST = '127.0.0.1';
const PORT = 3000;

const PACKET_SIZE = 17;
const CALL_TYPE_STREAM_ALL = 1;
const CALL_TYPE_RESEND = 2;

let receivedPackets = {};
let maxSequence = 0;

function streamAllPackets() {
  const client = new net.Socket();
  client.connect(PORT, HOST, () => {
    const payload = Buffer.alloc(2);
    payload.writeUInt8(CALL_TYPE_STREAM_ALL, 0);
    payload.writeUInt8(0, 1);
    client.write(payload);
  });

  let buffer = Buffer.alloc(0);

  client.on('data', (data) => {
    buffer = Buffer.concat([buffer, data]);
    while (buffer.length >= PACKET_SIZE) {
      const packet = buffer.slice(0, PACKET_SIZE);
      parsePacket(packet);
      buffer = buffer.slice(PACKET_SIZE);
    }
  });

  client.on('end', () => {
    requestMissingPackets();
  });

  client.on('error', (err) => {
    console.error('Connection error:', err.message);
  });
}

function parsePacket(buffer) {
  const symbol = buffer.slice(0, 4).toString('ascii');
  const side = buffer.slice(4, 5).toString('ascii');
  const quantity = buffer.readInt32BE(5);
  const price = buffer.readInt32BE(9);
  const seq = buffer.readInt32BE(13);

  receivedPackets[seq] = {
    symbol,
    side,
    quantity,
    price,
    sequence: seq,
  };

  if (seq > maxSequence) maxSequence = seq;
}

function requestMissingPackets() {
  const missingSeqs = [];
  for (let i = 1; i <= maxSequence; i++) {
    if (!receivedPackets[i]) missingSeqs.push(i);
  }

  if (missingSeqs.length === 0) {
    return saveToFile();
  }

  let completed = 0;
  missingSeqs.forEach(seq => {
    const client = new net.Socket();
    client.connect(PORT, HOST, () => {
      const payload = Buffer.alloc(2);
      payload.writeUInt8(CALL_TYPE_RESEND, 0);
      payload.writeUInt8(seq, 1);
      client.write(payload);
    });

    let buffer = Buffer.alloc(0);
    client.on('data', (data) => {
      buffer = Buffer.concat([buffer, data]);
      if (buffer.length >= PACKET_SIZE) {
        parsePacket(buffer.slice(0, PACKET_SIZE));
        client.end();
      }
    });

    client.on('end', () => {
      completed++;
      if (completed === missingSeqs.length) {
        saveToFile();
      }
    });

    client.on('error', (err) => {
      console.error(`Error for seq ${seq}:`, err.message);
    });
  });
}

function saveToFile() {
  const packets = Object.values(receivedPackets).sort((a, b) => a.sequence - b.sequence);
  fs.writeFileSync('data.json', JSON.stringify(packets, null, 2));
}

streamAllPackets();
