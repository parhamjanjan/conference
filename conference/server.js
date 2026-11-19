
'use strict';

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { ExpressPeerServer } = require('peer');
const path = require('path');

const app = express();
const server = http.createServer(app);

// =====================================
// SOCKET.IO SERVER
// =====================================

const io = new Server(server, {
    cors: {
        origin: '*',
        methods: ['GET', 'POST']
    },
    transports: ['websocket'],
    maxHttpBufferSize: 64 * 1024,
    perMessageDeflate: false,
    pingInterval: 25000,
    pingTimeout: 20000
});

// =====================================
// PEERJS SERVER
// =====================================

const peerServer = ExpressPeerServer(server, {
    proxied: true,
    debug: 2
});

app.use('/peerjs', peerServer);

peerServer.on('connection', (client) => {
    console.log(`[PeerJS] Connected: ${client.getId()}`);
});

peerServer.on('disconnect', (client) => {
    console.log(`[PeerJS] Disconnected: ${client.getId()}`);
});

// =====================================
// STATIC FILES
// =====================================

app.use(express.static(path.join(__dirname, 'public'), {
    etag: true,
    maxAge: '1h'
}));

// =====================================
// ROOM STORAGE
// =====================================

// rooms[roomName] = Map<socketId, memberInfo>

const rooms = new Map();

const MAX_ROOM_NAME = 40;
const MAX_NAME = 30;
const MAX_PROFILE_SIZE = 20 * 1024;

// =====================================
// HELPERS
// =====================================

function cleanText(value, maxLength) {
    return String(value || '')
        .replace(/[<>\u0000-\u001F]/g, '')
        .trim()
        .slice(0, maxLength);
}

function isValidUserId(value) {
    return typeof value === 'string' &&
        /^[a-f0-9]{64}$/i.test(value);
}

function broadcastMemberList(room) {
    const members = rooms.get(room);

    if (!members) return;

    const list = [];

    for (const [socketId, info] of members) {
        list.push({
            socketId,
            peerId: info.peerId,
            userId: info.userId,
            name: info.name,
            camOn: info.camOn,
            studying: info.studying
        });
    }

    io.to(room).emit('member-list', list);
}

function getMember(socket) {
    if (!socket.room) return null;

    const members = rooms.get(socket.room);

    if (!members) return null;

    return members.get(socket.id) || null;
}

// =====================================
// SOCKET.IO CONNECTION
// =====================================

io.on('connection', (socket) => {

    // =================================
    // JOIN ROOM
    // =================================

    socket.on('join', (payload = {}) => {
        if (socket.room) return;

        const room = cleanText(payload.room, MAX_ROOM_NAME)
            .replace(/[^a-zA-Z0-9_-]/g, '');

        const peerId = cleanText(payload.peerId, 100);

        const name = cleanText(payload.name, MAX_NAME) || 'User';

        const userId = payload.userId;

        if (!room || !peerId || !isValidUserId(userId)) {
            socket.emit('join-error', {
                message: 'Invalid room, peer ID or user ID.'
            });

            return;
        }

        if (!rooms.has(room)) {
            rooms.set(room, new Map());
        }

        const members = rooms.get(room);

        if (members.has(socket.id)) return;

        const memberInfo = {
            peerId,
            userId,
            name,
            camOn: false,
            studying: false
        };

        members.set(socket.id, memberInfo);

        socket.join(room);

        socket.room = room;
        socket.peerId = peerId;
        socket.userId = userId;

        // Send existing members to the new user.

        const others = [];

        for (const [otherSocketId, info] of members) {
            if (otherSocketId === socket.id) continue;

            others.push({
                socketId: otherSocketId,
                peerId: info.peerId,
                userId: info.userId,
                name: info.name,
                camOn: info.camOn,
                studying: info.studying
            });
        }

        socket.emit('others', others);

        // Notify existing users.

        socket.to(room).emit('peer-joined', {
            socketId: socket.id,
            peerId,
            userId,
            name,
            camOn: false,
            studying: false
        });

        // Request existing users' study profiles.

        socket.to(room).emit('study-profile-request', {
            requesterUserId: userId
        });

        broadcastMemberList(room);

        console.log(
            `[${room}] ${name} joined. Members: ${members.size}`
        );
    });

    // =================================
    // CAMERA STATUS
    // =================================

    socket.on('cam-status', (payload = {}) => {
        const info = getMember(socket);

        if (!info) return;

        info.camOn = payload.camOn === true;

        socket.to(socket.room).emit('peer-cam-status', {
            peerId: info.peerId,
            camOn: info.camOn
        });

        broadcastMemberList(socket.room);
    });

    // =================================
    // STUDY STATUS
    // =================================

    socket.on('study-status', (payload = {}) => {
        const info = getMember(socket);

        if (!info) return;

        info.studying = payload.studying === true;

        socket.to(socket.room).emit('peer-study-status', {
            userId: info.userId,
            peerId: info.peerId,
            studying: info.studying
        });

        broadcastMemberList(socket.room);
    });

    // =================================
    // REQUEST STUDY PROFILES
    // =================================

    socket.on('study-profile-request', () => {
        const info = getMember(socket);

        if (!info) return;

        socket.to(socket.room).emit('study-profile-request', {
            requesterUserId: info.userId
        });
    });

    // =================================
    // TRANSFER STUDY PROFILES
    // =================================

    socket.on('study-profile', (payload = {}) => {
        const info = getMember(socket);

        if (!info) return;

        const data = payload.data;

        if (
            !data ||
            typeof data !== 'object' ||
            Array.isArray(data)
        ) {
            return;
        }

        let serialized;

        try {
            serialized = JSON.stringify(data);
        } catch {
            return;
        }

        if (
            !serialized ||
            Buffer.byteLength(serialized, 'utf8') > MAX_PROFILE_SIZE
        ) {
            return;
        }

        // The server does not store study profiles.
        // It only relays them to other room members.

        socket.to(socket.room).emit('study-profile', {
            userId: info.userId,
            name: info.name,
            data
        });
    });

    // =================================
    // DISCONNECT
    // =================================

    socket.on('disconnect', () => {
        const room = socket.room;

        if (!room) return;

        const members = rooms.get(room);

        if (!members) return;

        const info = members.get(socket.id);

        members.delete(socket.id);

        socket.to(room).emit('peer-left', {
            socketId: socket.id,
            peerId: socket.peerId,
            userId: socket.userId
        });

        if (members.size === 0) {
            rooms.delete(room);
        } else {
            broadcastMemberList(room);
        }

        console.log(
            `[${room}] ${info?.name || socket.peerId} left.`
        );
    });

});

// =====================================
// HEALTH CHECK
// =====================================

app.get('/health', (_req, res) => {
    res.status(200).json({
        ok: true,
        service: 'focus-hall-signaling',
        socketIO: true,
        peerJS: true,
        peerJSPath: '/peerjs',
        storage: 'browser-localStorage'
    });
});

// =====================================
// START SERVER
// =====================================

const PORT = Number(process.env.PORT) || 3000;

server.listen(PORT, '0.0.0.0', () => {
    console.log('====================================');
    console.log('Focus Hall Server Started');
    console.log(`Port: ${PORT}`);
    console.log('Socket.IO: Enabled');
    console.log('PeerJS: Enabled');
    console.log('PeerJS Path: /peerjs');
    console.log('====================================');
});
