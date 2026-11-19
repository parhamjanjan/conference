const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: '*', methods: ['GET', 'POST'] }
});

app.use(express.static(path.join(__dirname, 'public')));

// rooms[roomName] = Map<socketId, { peerId, name, camOn }>
const rooms = {};

io.on('connection', (socket) => {
    socket.on('join', ({ room, peerId, name }) => {
        if (!rooms[room]) rooms[room] = new Map();

        // ذخیره اطلاعات کاربر
        rooms[room].set(socket.id, { peerId, name, camOn: false });
        socket.join(room);
        socket.room = room;
        socket.peerId = peerId;

        // لیست بقیه رو بفرست (با اسم و وضعیت وبکم)
        const others = [];
        for (const [sid, info] of rooms[room]) {
            if (sid !== socket.id) {
                others.push({
                    socketId: sid,
                    peerId: info.peerId,
                    name: info.name,
                    camOn: info.camOn
                });
            }
        }

        socket.emit('others', others);

        // به بقیه بگو کاربر جدید اومد
        socket.to(room).emit('peer-joined', {
            socketId: socket.id,
            peerId,
            name,
            camOn: false
        });

        // لیست کامل رو برای همه بفرست تا UI آپدیت بشه
        broadcastMemberList(room);

        console.log(`[${room}] ${name} (${peerId}) joined. Total: ${rooms[room].size}`);
    });

    // وقتی وبکم کاربر روشن/خاموش شد، به بقیه اطلاع بده
    socket.on('cam-status', ({ camOn }) => {
        const room = socket.room;
        if (!room || !rooms[room]) return;
        const info = rooms[room].get(socket.id);
        if (info) {
            info.camOn = camOn;
            socket.to(room).emit('peer-cam-status', {
                peerId: socket.peerId,
                camOn
            });
            broadcastMemberList(room);
        }
    });

    socket.on('disconnect', () => {
        const room = socket.room;
        if (room && rooms[room]) {
            rooms[room].delete(socket.id);
            socket.to(room).emit('peer-left', {
                socketId: socket.id,
                peerId: socket.peerId
            });
            if (rooms[room].size === 0) {
                delete rooms[room];
            } else {
                broadcastMemberList(room);
            }
            console.log(`[${room}] ${socket.peerId} left.`);
        }
    });
});

function broadcastMemberList(room) {
    if (!rooms[room]) return;
    const list = [];
    for (const [sid, info] of rooms[room]) {
        list.push({
            socketId: sid,
            peerId: info.peerId,
            name: info.name,
            camOn: info.camOn
        });
    }
    io.to(room).emit('member-list', list);
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 Server on port ${PORT}`));