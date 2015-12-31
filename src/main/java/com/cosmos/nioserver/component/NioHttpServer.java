package com.cosmos.nioserver.component;

import com.cosmos.nioserver.protocol.ChangeRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;

/**
 * 基于NIO的HTTP Server调度线程
 */
public class NioHttpServer implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(NioHttpServer.class);

    private ServerSocketChannel serverChannel;
    private Selector selector;
    private ByteBuffer readBuffer = ByteBuffer.allocate(8912);
    private final List<ChangeRequest> changeRequests = new LinkedList<>();
    private final Map<SocketChannel, List<ByteBuffer>> pendingSent = new HashMap<>();
    private List<RequestHandler> requestHandlers = new ArrayList<>();

    public NioHttpServer(InetAddress address, int port) throws IOException {
        selector = Selector.open();
        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.socket().bind(new InetSocketAddress(address, port));
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

    }

    private void accept(SelectionKey key) throws IOException {
        SocketChannel socketChannel = serverChannel.accept();
        logger.info("new connection:\t" + socketChannel);
        socketChannel.configureBlocking(false);
        socketChannel.register(selector, SelectionKey.OP_READ);
    }

    public void addRequestHandler(RequestHandler handler) {
        requestHandlers.add(handler);
    }

    private void read(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        readBuffer.clear();
        int numRead;
        try {
            numRead = socketChannel.read(readBuffer);
        } catch (IOException e) {
            // the remote forcibly closed the connection
            key.cancel();
            socketChannel.close();
            logger.info("closed by exception" + socketChannel);
            return;
        }

        if (numRead == -1) {
            // remote entity shut the socket down cleanly.
            socketChannel.close();
            key.cancel();
            logger.info("closed by shutdown" + socketChannel);
            return;
        }

        int worker = socketChannel.hashCode() % requestHandlers.size();
        if (logger.isDebugEnabled()) {
            logger.debug(selector.keys().size() + "\t" + worker + "\t" + socketChannel);
        }
        requestHandlers.get(worker).processData(socketChannel, readBuffer.array(), numRead);
    }

    @Override
    public void run() {
        SelectionKey key = null;
        while (true) {
            try {
                synchronized (changeRequests) {
                    for (ChangeRequest request : changeRequests) {
                        switch (request.type) {
                            case ChangeRequest.CHANGEOPS:
                                key = request.socket.keyFor(selector);
                                if (key != null && key.isValid()) {
                                    key.interestOps(request.ops);
                                }
                                break;
                        }
                    }
                    changeRequests.clear();
                }

                selector.select();
                Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();
                while (selectedKeys.hasNext()) {
                    key = selectedKeys.next();
                    selectedKeys.remove();
                    if (!key.isValid()) {
                        continue;
                    }
                    if (key.isAcceptable()) {
                        accept(key);
                    } else if (key.isReadable()) {
                        read(key);
                    } else if (key.isWritable()) {
                        write(key);
                    }
                }
            } catch (Exception e) {
                if (key != null) {
                    key.cancel();
                    if (key.channel() != null) {
                        try {
                            key.channel().close();
                        } catch (IOException e1) {
                        }

                    }
                }
                logger.error("closed" + key.channel(), e);
            }
        }

    }

    public void send(SocketChannel socket, byte[] data) {
        synchronized (changeRequests) {
            changeRequests.add(new ChangeRequest(socket, ChangeRequest.CHANGEOPS, SelectionKey.OP_WRITE));
            synchronized (pendingSent) {
                List<ByteBuffer> queue = pendingSent.get(socket);
                if (queue == null) {
                    queue = new ArrayList<>();
                    pendingSent.put(socket, queue);
                }
                queue.add(ByteBuffer.wrap(data));
            }
        }

        selector.wakeup();
    }

    private void write(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        synchronized (pendingSent) {
            List<ByteBuffer> queue = pendingSent.get(socketChannel);
            while (!queue.isEmpty()) {
                ByteBuffer buf = queue.get(0);
                socketChannel.write(buf);
                // have more to send
                if (buf.remaining() > 0) {
                    break;
                }
                queue.remove(0);
            }
            if (queue.isEmpty()) {
                key.interestOps(SelectionKey.OP_READ);
            }
        }
    }
}
