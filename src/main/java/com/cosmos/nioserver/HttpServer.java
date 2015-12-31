package com.cosmos.nioserver;

import com.cosmos.nioserver.component.SoftCache;
import com.cosmos.nioserver.component.NioHttpServer;
import com.cosmos.nioserver.component.RequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * HTTP Server启动类
 */
public class HttpServer {

    private static final Logger logger = LoggerFactory.getLogger(HttpServer.class);

    public static void main(String[] args) throws IOException {

        String root = new File(".").getAbsolutePath();
        int port = 8080;
        if (args.length > 0)
            port = Integer.parseInt(args[0]);

        if (args.length > 1)
            root = args[1];

        logger.info("Starting HTTP Server...");
        logger.info("using port: {}", port);
        logger.info("root path:  {}", root);

        NioHttpServer server = new NioHttpServer(null, port);
        // 获取CPU数量
        int cpu = Runtime.getRuntime().availableProcessors();
        SoftCache cache = new SoftCache();
        // int i = 0;
        for (int i = 0; i < cpu; ++i) {
            RequestHandler handler = new RequestHandler(server, root, cache);
            server.addRequestHandler(handler);
            new Thread(handler, "worker" + i).start();
        }

        new Thread(server, "selector").start();

        logger.info("HTTP Server started...");
    }
}
