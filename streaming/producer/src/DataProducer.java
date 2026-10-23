import java.io.*;
import java.net.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DataProducer {
    private static final int PORT = 9999;
    private static final Random random = new Random();
    private static final long INTERVAL_MS = Long.parseLong(System.getenv().getOrDefault("EVENT_INTERVAL_MS", "100"));
    private static final String RUN_ID = UUID.randomUUID().toString();

    public static void main(String[] args) throws IOException {
        if (INTERVAL_MS < 1) throw new IllegalArgumentException("EVENT_INTERVAL_MS must be positive");
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try (ServerSocket serverSocket = new ServerSocket(PORT, 0, InetAddress.getByName("0.0.0.0"))) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> pool.shutdown()));
            System.out.println("Producer started on port " + PORT);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                pool.execute(new ClientHandler(clientSocket));
            }
        }
    }

    static class ClientHandler implements Runnable {
        private final Socket socket;

        ClientHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try (Socket connection = socket; PrintWriter out = new PrintWriter(connection.getOutputStream(), true)) {
                while (!socket.isClosed()) {
                    out.println(generateLogEntry());
                    if (out.checkError()) break;
                    Thread.sleep(INTERVAL_MS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                System.err.println("Client connection error: " + e.getMessage());
            }
        }

        private String generateLogEntry() {
            String[] levels = {"Information", "Warning", "Error"};
            String[] sources = {"Security", "System", "Application"};
            int[] securityIds = {4624, 4625, 4648};
            int[] systemIds = {7036, 7040, 7045};
            int[] appIds = {1000, 1001, 1002};

            String source = sources[random.nextInt(sources.length)];
            int eventId = switch (source) {
                case "Security" -> securityIds[random.nextInt(securityIds.length)];
                case "System" -> systemIds[random.nextInt(systemIds.length)];
                default -> appIds[random.nextInt(appIds.length)];
            };

            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                                                       .withZone(ZoneId.of("UTC"));

            return String.format(
                "{\"eventUid\":\"%s\",\"pipelineRun\":\"%s\",\"generatedBy\":\"java-tcp-producer\",\"eventId\":%d,\"level\":\"%s\",\"source\":\"%s\",\"timestamp\":\"%s\",\"computer\":\"%s\",\"user\":\"%s\",\"message\":\"%s\"}",
                UUID.randomUUID().toString(),
                RUN_ID,
                eventId,
                levels[random.nextInt(levels.length)],
                source,
                formatter.format(Instant.now()),
                "SERVER" + (random.nextInt(5) + 1),
                random.nextBoolean() ? "SYSTEM" : "ADMIN",
                "Simulated Windows event: " + source + " " + eventId
            );
        }
    }
}
