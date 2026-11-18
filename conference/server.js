const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

app.use(express.static(path.join(__dirname, 'public')));

const rooms = {};

io.on('connection', (socket) => {
    socket.on('join', ({ room, peerId }) => {
        if (!rooms[room]) rooms[room] = new Set();
        rooms[room].add(socket.id);
        socket.join(room);
        socket.room = room;
        socket.peerId = peerId;

        const others = [...rooms[room]]
            .filter(id => id !== socket.id)
            .map(id => {
                const s = io.sockets.sockets.get(id);
                return s ? { socketId: id, peerId: s.peerId } : null;
            }).filter(Boolean);

        socket.emit('others', others);
        socket.to(room).emit('peer-joined', { socketId: socket.id, peerId });
    });

    socket.on('disconnect', () => {
        if (socket.room && rooms[socket.room]) {
            rooms[socket.room].delete(socket.id);
            socket.to(socket.room).emit('peer-left', { socketId: socket.id, peerId: socket.peerId });
            if (rooms[socket.room].size === 0) delete rooms[socket.room];
        }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('Server on port ' + PORT));