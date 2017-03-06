import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;


class Client {

    private int port;
    private String host;
    private String protocol;

    private int nThreads;
    private int pause;
    private int msgLength;

    private final static AtomicLong nSent = new AtomicLong();

    private static final Random RND = new Random();
    // random string charset
    private static final String CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final String ENDL = System.getProperty("line.separator");

    // expecting to receive a responce in 120 seconds
    private static final int TIMEOUT = 120000;
    // maximum client pause = 20 seconds;
    private static final int MAX_PAUSE = 20000;

    private static final String PROP_FILE = "client.properties";


    // generate random string of length n
    private static String generateMessage(int n) {

        StringBuilder sb = new StringBuilder();
        while (sb.length() < n) {
            int i = (int) (RND.nextFloat() * CHARS.length());
            sb.append(CHARS.charAt(i));
        }
        return sb.toString() + ENDL;
    }

    // read client settings
    private void readSettings() {

        InputStream iProps =
                Client.class.getClassLoader().getResourceAsStream(PROP_FILE);
        if (iProps == null) {
            System.err.println("cannot read " + PROP_FILE);
            System.exit(1);
        }

        Properties config = new Properties();
        try {
            config.load(iProps);
        } catch (IOException e) {
            System.err.println(
                    "error loading " + PROP_FILE + ": " + e.getMessage());
            System.exit(1);
        }

        System.out.println("reading configuration parameters from " + PROP_FILE);

        host = config.getProperty("serverIP");
        if (host == null || host.isEmpty()) {
            System.err.println(
                "please specify server host IP in configuration file");
            System.exit(1);
        }
        System.out.println("server host = " + host);

        String sPort = config.getProperty("serverPort");
        if (sPort == null || sPort.isEmpty()) {
            System.err.println(
                "please specify server port in configuration file");
            System.exit(1);
        }
        port = Integer.parseInt(sPort);
        if (port < 0x0400 || port > 0xffff) {
            System.err.println(
                    "please select port number from range [0x0400 .. 0xffff]");
            System.exit(1);
        }
        System.out.println("server port = " + port);

        protocol = config.getProperty("protocol");
        if (protocol == null || protocol.isEmpty()) {
            System.err.println(
                "please specify protocol in configuration file");
            System.exit(1);
        }
        protocol = protocol.trim().toUpperCase();
        if (!(protocol.equals("TCP") || protocol.equals("UDP"))) {
            System.err.println("unsupported protocol: " + protocol);
            System.exit(1);
        }
        System.out.println("protocol = " + protocol);


        String sNThreads = config.getProperty("nClientThreads");
        if (sNThreads == null || sNThreads.isEmpty()) {
            System.err.println(
                "please specify number of client threads in configuration file");
            System.exit(1);
        }
        nThreads = Integer.parseInt(sNThreads);
        if (nThreads < 1) {
            System.err.println("at least one client thread must exist");
            System.exit(1);
        }
        System.out.println("number of client threads = " + nThreads);

        String sPause = config.getProperty("clientThreadPause");
        if (sPause == null || sPause.isEmpty()) {
            System.err.println(
                    "please specify client pause in configuration file");
            System.exit(1);
        }
        pause = Integer.parseInt(sPause);
        if (pause < 1 || pause > MAX_PAUSE) {
            System.err.println("client pause must lie in range [1, " +
                    MAX_PAUSE + "] milliseconds");
            System.exit(1);
        }
        System.out.println("pause = " + pause + " milliseconds");

        String sMsgLength = config.getProperty("messageLength");
        if (sMsgLength == null || sMsgLength.isEmpty()) {
            System.err.println(
                    "please specify message length in configuration file");
            System.exit(1);
        }
        msgLength = Integer.parseInt(sMsgLength);
        if (msgLength < 1) {
            System.err.println("message length must be positive");
            System.exit(1);
        }
        System.out.println("message length = " + msgLength);
    }

    // need this to get received messages count on Ctrl + C
    private void addNSentHook() {
        // write number of received messages on exit
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("\n\n" + nSent.get() + " messages sent");
            }
        });
    }

    public void start() {

        readSettings();
        addNSentHook();

        for (int i = 0; i < nThreads; ++i) {
            if (protocol.equals("TCP")) {
                (new Thread(
                        new TCPClient(i + 1,
                                     host,
                                     port,
                                     pause,
                                     msgLength)
                )).start();
            } else if (protocol.equals("UDP")) {
                (new Thread(
                        new UDPClient(i + 1,
                                     host,
                                     port,
                                     pause,
                                     msgLength)
                )).start();
            } // else {} -- not reachable
        }
    }

    public static void main(String argv[]) { (new Client()).start(); }

    private static class TCPClient implements Runnable {

        private final int numThread;
        private final String host;
        private final int port;
        private final int pause;
        private final int messageLength;

        public TCPClient(int        numThread,
                         String     host,
                         int        port,
                         int        pause,
                         int        length) {
            this.numThread = numThread;
            this.host = host;
            this.port = port;
            this.pause = pause;
            this.messageLength = length;
        }

        private boolean exchange() {

            String sentence = generateMessage(messageLength); // + " from #" + numThread;
            String response;

            try (Socket socket = new Socket(host, port)) {

                socket.setSoTimeout(TIMEOUT);

                DataOutputStream toServer =
                        new DataOutputStream(socket.getOutputStream());
                BufferedReader fromServer = new BufferedReader(
                        new InputStreamReader(socket.getInputStream()));

                toServer.writeBytes(sentence);
                nSent.incrementAndGet();

                response = fromServer.readLine();

            } catch (IOException e) {

                System.err.println("client #" + numThread + ": " +
                        "TCP exchange failed, " + e.getMessage());
                return false;
            }

            if (response != null) {
                System.out.println(sentence + " -> " + response);
            }

            return true;
        }

        @Override
        public void run() {

            boolean ok = true;
            while (ok) {
                ok = exchange();
                try { Thread.sleep(pause); }
                catch (InterruptedException dummy) {}
            }
        }
    }

    private static class UDPClient implements Runnable {

        private final int numThread;
        private final String host;
        private final int port;
        private final int pause;
        private final int messageLength;

        public UDPClient(int        numThread,
                         String     host,
                         int        port,
                         int        pause,
                         int        length) {
            this.numThread = numThread;
            this.host = host;
            this.port = port;
            this.pause = pause;
            this.messageLength = length;
        }


        private boolean exchange() {

            String sentence = generateMessage(messageLength); // + " from #" + numThread;
            String response;

            try (DatagramSocket socket = new DatagramSocket()) {

                socket.setSoTimeout(TIMEOUT);

                byte bytes[] = sentence.getBytes();

                DatagramPacket packet = new DatagramPacket(
                    bytes, bytes.length, InetAddress.getByName(host), port);

                socket.send(packet);
                nSent.incrementAndGet();

                byte[] inBuff = new byte[1024]; // definitely enough for "OK\n"
                DatagramPacket receivePacket =
                        new DatagramPacket(inBuff, inBuff.length);
                socket.receive(receivePacket);

                response = new String(receivePacket.getData()).trim();
            } catch (IOException e) {
                System.err.println("client thread " + numThread + ": " +
                        "UDP exchange failed, " + e.getMessage());
                return false;
            }

            if (response != null) {
                System.out.println(sentence + " -> " + response);
            }

            return true;
        }

        @Override
        public void run() {

            boolean ok = true;
            while (ok) {
                ok = exchange();
                try { Thread.sleep(pause); }
                catch (InterruptedException dummy) {}
            }
        }
    }
}
