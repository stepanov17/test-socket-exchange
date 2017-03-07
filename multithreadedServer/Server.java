import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;


public class Server {

    // FIXME: hopefully multi-thread logging is safe here. At least, looking at
    // the Logger's docs: "All methods on Logger are multi-thread safe."
    private static final Logger LOGGER = Logger.getLogger("serverLogger");
    private static final String LOG_FILE = "server.log";

    private static final String PROP_FILE = "server.properties";

    private static final String RESPONSE = "OK\n";

    // maximum server delay = 20 seconds;
    // in case of increase please fix also timeouts in the client
    private static final int MAX_DELAY = 20000;

    private static final String LN =
            "__________________________________________________________________";
    private static final String ENDL = System.getProperty("line.separator");

    private static final int MAX_BACKLOG = 0xffff;
    private static final int UDP_BUFF_SIZE = 0xffff;



    private int port;
    private String protocol;
    private int delay;
    private InetAddress address;

    private final static AtomicLong nReceived = new AtomicLong();



    // initialize logger - do that at 1st!
    private void initLogger() {

        FileHandler fh;
        try {
            fh = new FileHandler(LOG_FILE);
            LOGGER.addHandler(fh);
            fh.setFormatter(new SimpleFormatter());
        } catch (IOException e) {
            System.err.println("cannot create logger: " + e.getMessage());
            System.exit(1);
        }
    }

    // read server configuration
    private void readSettings() {

        InputStream iProps =
                Server.class.getClassLoader().getResourceAsStream(PROP_FILE);
        if (iProps == null) {
            LOGGER.log(Level.SEVERE, "cannot read " + PROP_FILE);
            System.exit(1);
        }

        Properties config = new Properties();
        try {
            config.load(iProps);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE,
                    "error loading " + PROP_FILE + ": " + e.getMessage());
            System.exit(1);
        }

        LOGGER.log(Level.INFO,
                "reading configuration parameters from " + PROP_FILE + ENDL);

        address = null;
        String sIP = config.getProperty("IP");
        if (sIP == null || sIP.isEmpty()) {
            LOGGER.log(Level.INFO, "IP address is not specified" + ENDL);
        } else {
            try {
                address = InetAddress.getByName(sIP);
                LOGGER.log(Level.INFO, "IP = " + sIP + ENDL);
            } catch (UnknownHostException e) {
                LOGGER.log(Level.SEVERE,
                        "cannot resolve address " + sIP + ": " + e.getMessage());
                System.exit(1);
            }
        }

        String sPort = config.getProperty("port");
        if (sPort == null || sPort.isEmpty()) {
            LOGGER.log(Level.SEVERE,
                "please specify port in configuration file");
            System.exit(1);
        }
        port = Integer.parseInt(sPort);
        // avoid privileged ports
        if (port < 0x0400 || port > 0xffff) {
            LOGGER.log(Level.SEVERE,
                    "please select port number from range [0x0400 .. 0xffff]");
            System.exit(1);
        }
        LOGGER.log(Level.INFO, "port = " + port + ENDL);

        protocol = config.getProperty("protocol");
        if (protocol == null || protocol.isEmpty()) {
            LOGGER.log(Level.SEVERE,
                "please specify protocol in configuration file");
            System.exit(1);
        }
        protocol = protocol.trim().toUpperCase();
        if (!(protocol.equals("TCP") || protocol.equals("UDP"))) {
            LOGGER.log(Level.SEVERE,
                "unsupported protocol: " + protocol);
            System.exit(1);
        }
        LOGGER.log(Level.INFO, "protocol = " + protocol + ENDL);

        String sDelay = config.getProperty("serverDelay");
        if (sDelay == null || sDelay.isEmpty()) {
            LOGGER.log(Level.SEVERE,
                "please specify server delay in configuration file");
            System.exit(1);
        }
        delay = Integer.parseInt(sDelay);
        if (delay < 1 || delay > MAX_DELAY) {
            LOGGER.log(Level.SEVERE, "server delay must lie in range [1, " +
                    MAX_DELAY + "] milliseconds");
            System.exit(1);
        }
        LOGGER.log(Level.INFO, "server delay = " + delay + " milliseconds" + ENDL);
    }

    // need this to get received messages count on Ctrl + C
    private void addNReceivedHook() {
        // write number of received messages on exit
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("\n\n" + nReceived.get() + " messages received");
                LOGGER.log(Level.INFO,
                        "number of received messages: " + nReceived.get());
            }
        });
    }

    private void runTCPLoop() {

        ServerSocket serverSocket;
        try {
            serverSocket = new ServerSocket(port, MAX_BACKLOG, address);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "cannot create server socket on port " +
                    Long.toString(port) + ": " + e.getMessage() + ENDL);
            return;
        }


        LOGGER.log(Level.INFO, "TCP server started on port " +
                Long.toString(port) + ENDL + LN + ENDL);

        while (true) {

            Socket connectionSocket;

            try {
                connectionSocket = serverSocket.accept();
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "error accepting the socket: " +
                        e.getMessage() + ENDL);
                continue;
            }

            (new Thread(new TCPHandler(connectionSocket, delay))).start();
        }
    }

    private void runUDPLoop() {

        DatagramSocket serverSocket;
        try {
            serverSocket = new DatagramSocket(port, address);
        } catch (SocketException e) {
            LOGGER.log(Level.SEVERE,
                    "cannot create server socket on port " + 
                    Long.toString(port) + ": " + e.getMessage() + ENDL);
            return;
        }

        LOGGER.log(Level.INFO, "UDP server started on port " +
                Long.toString(port) + ENDL + LN + ENDL);

        byte inData[] = new byte[UDP_BUFF_SIZE];

        while (true) {

            DatagramPacket receivePacket =
                    new DatagramPacket(inData, UDP_BUFF_SIZE);
            try {
                serverSocket.receive(receivePacket);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "error while receiving a packet: " +
                        e.getMessage() + ENDL);
                continue;
            }

            (new Thread(new UDPHandler(
                    serverSocket, receivePacket, delay))).start();
        }
    }

    public void start() {

        // initialize
        initLogger();
        readSettings();
        addNReceivedHook();

        // start listening
        if (protocol.equals("TCP")) { runTCPLoop(); }
        else if (protocol.equals("UDP")) { runUDPLoop(); }
        // else {} -- not reachable
    }

    private static class TCPHandler implements Runnable {

        private final Socket connectionSocket;
        private final int handlerDelay;
        public TCPHandler(Socket s, int delay) {
            connectionSocket = s;
            handlerDelay = delay;
        }

        @Override
        public void run() {

            try {
                BufferedReader fromClient = new BufferedReader(
                    new InputStreamReader(connectionSocket.getInputStream()));
                String clientSentence = fromClient.readLine();
                nReceived.incrementAndGet();
                LOGGER.log(Level.INFO, "received " + clientSentence + ENDL);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING,
                        "error while reading from client socket: " +
                        e.getMessage() + ENDL);
                return;
            }

            try { Thread.sleep(handlerDelay); }
            catch (InterruptedException dummy) {}

            try {
                DataOutputStream toClient =
                new DataOutputStream(connectionSocket.getOutputStream());
                toClient.writeBytes(RESPONSE);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING,
                        "error while writing to client socket: " +
                        e.getMessage() + ENDL);
            }
        }
    }

    private static class UDPHandler implements Runnable {

        private final DatagramSocket socket;
        private final DatagramPacket receivePacket;
        private final int handlerDelay;

        public UDPHandler(DatagramSocket s, DatagramPacket p, int delay) {
            socket = s;
            receivePacket = p;
            handlerDelay = delay;
        }

        @Override
        public void run() {

            String clientSentence =
                    (new String(receivePacket.getData())).trim();
            nReceived.incrementAndGet();
            LOGGER.log(Level.INFO, "received " + clientSentence + ENDL);

            try { Thread.sleep(handlerDelay); }
            catch (InterruptedException dummy) {}

            byte reData[] = RESPONSE.getBytes();
            DatagramPacket sendPacket = new DatagramPacket(
                    reData, reData.length,
                    receivePacket.getAddress(), receivePacket.getPort());
            try {
                socket.send(sendPacket);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "error while sending a packet: " +
                        e.getMessage() + ENDL);
            }
        }
    }

    public static void main(String argv[]) { (new Server()).start(); }
}
