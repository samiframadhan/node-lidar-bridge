const zmq = require('zeromq');
const msgpack = require('msgpack5')();
const express = require('express');
const { createServer } = require('http');
const { Server } = require('socket.io');
const path = require('path');

// Configuration
const ZMQ_PORT = 5551;
const HTTP_PORT = 3000;
const LIDAR_TOPIC = 'tcp://127.0.0.1:5551';

class LidarBridge {
    constructor() {
        this.app = express();
        this.server = createServer(this.app);
        this.io = new Server(this.server, {
            cors: {
                origin: "*",
                methods: ["GET", "POST"]
            }
        });
        
        this.subscriber = null;
        this.isRunning = false;
        this.connectedClients = 0;
        
        this.setupExpress();
        this.setupSocketIO();
        this.setupZMQ();
    }

    setupExpress() {
        // Serve static files from current directory
        this.app.use(express.static(__dirname));
        
        // Route for the main page
        this.app.get('/', (req, res) => {
            res.sendFile(path.join(__dirname, 'index.html'));
        });

        // Health check endpoint
        this.app.get('/health', (req, res) => {
            res.json({
                status: 'ok',
                zmq_connected: this.isRunning,
                clients_connected: this.connectedClients,
                timestamp: new Date().toISOString()
            });
        });
    }

    setupSocketIO() {
        this.io.on('connection', (socket) => {
            this.connectedClients++;
            console.log(`Client connected. Total clients: ${this.connectedClients}`);

            socket.on('disconnect', () => {
                this.connectedClients--;
                console.log(`Client disconnected. Total clients: ${this.connectedClients}`);
            });

            // Send initial connection confirmation
            socket.emit('status', { 
                message: 'Connected to LIDAR bridge',
                timestamp: new Date().toISOString()
            });
        });
    }

    async setupZMQ() {
        try {
            // Create ZMQ subscriber socket
            this.subscriber = new zmq.Subscriber();
            
            // Connect to the Rust publisher
            console.log(`Connecting to LIDAR data source at ${LIDAR_TOPIC}...`);
            this.subscriber.connect(LIDAR_TOPIC);
            
            // Subscribe to all messages (empty string means all)
            this.subscriber.subscribe('');
            
            console.log('ZMQ subscriber connected successfully');
            this.isRunning = true;
            
            // Start listening for messages
            this.startListening();
            
        } catch (error) {
            console.error('Failed to setup ZMQ:', error);
            this.isRunning = false;
        }
    }

    async startListening() {
        console.log('Starting to listen for LIDAR data...');
        
        try {
            for await (const [msg] of this.subscriber) {
                try {
                    // Deserialize MessagePack data
                    const lidarPoints = msgpack.decode(msg);
                    
                    // Debug: Log raw data structure
                    if (Array.isArray(lidarPoints) && lidarPoints.length > 0) {
                        console.log('Raw LIDAR point sample:', lidarPoints[0]);
                        console.log('Point count:', lidarPoints.length);
                        console.log('First few field names:', Object.keys(lidarPoints[0] || {}));
                    }
                    
                    // Validate data structure
                    if (Array.isArray(lidarPoints) && lidarPoints.length > 0) {
                        // Process and broadcast to all connected clients
                        this.broadcastLidarData(msg, lidarPoints);
                    } else {
                        console.log('Invalid or empty LIDAR data received');
                    }
                    
                } catch (decodeError) {
                    console.error('Error decoding MessagePack data:', decodeError);
                }
            }
        } catch (error) {
            console.error('Error in ZMQ message loop:', error);
            this.isRunning = false;
        }
    }

    // In LidarBridge class, replace the entire function with this one.

    broadcastLidarData(rawMsg, decodedPoints) {
        if (this.connectedClients === 0) {
            return; // No clients to send data to
        }

        try {
            // Create a payload to send. We use the decoded data for the point count
            // but send the original raw message buffer for efficiency.
            const payload = {
                timestamp: Date.now(),
                pointCount: decodedPoints.length,
                points: rawMsg // This is the raw msgpack Buffer
            };

            // Broadcast the payload to all connected clients
            this.io.emit('lidar_scan', payload);

            // Log stats periodically
            if (Math.random() < 0.01) { // ~1% of the time
                console.log(`Broadcasted scan: ${decodedPoints.length} points to ${this.connectedClients} clients`);
            }

        } catch (error) {
            console.error('Error broadcasting LIDAR data:', error);
        }
    }

    start() {
        this.server.listen(HTTP_PORT, () => {
            console.log(`LIDAR Bridge Server running on http://localhost:${HTTP_PORT}`);
            console.log(`Waiting for LIDAR data from Rust application...`);
            console.log(`Press Ctrl+C to stop`);
        });
    }

    async stop() {
        console.log('Shutting down LIDAR bridge...');
        
        if (this.subscriber) {
            await this.subscriber.close();
        }
        
        this.server.close(() => {
            console.log('Server closed');
            process.exit(0);
        });
    }
}

// Handle graceful shutdown
process.on('SIGINT', async () => {
    console.log('\nReceived SIGINT, shutting down gracefully...');
    if (bridge) {
        await bridge.stop();
    }
});

process.on('SIGTERM', async () => {
    console.log('Received SIGTERM, shutting down gracefully...');
    if (bridge) {
        await bridge.stop();
    }
});

// Create and start the bridge
const bridge = new LidarBridge();
bridge.start();