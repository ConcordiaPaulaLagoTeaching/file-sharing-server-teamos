package ca.concordia.server;

import ca.concordia.filesystem.FileSystemManager;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Base64;

/**
 * FileServer is a minimal TCP server exposing filesystem operations over a line-based protocol:
 *
 * Protocol (one command per line):
 *   - CREATE <filename>
 *   - WRITE  <filename> <base64-encoded-bytes>
 *   - READ   <filename> => replies with base64 payload on success
 *   - DELETE <filename>
 *   - LIST              => replies with comma-separated filenames
 *   - QUIT              => disconnect this client (this implementation returns from start())
 */
public class FileServer {

    private FileSystemManager fsManager;
    private int port;

    /**
     * @param port            listening port (NOTE: start() currently binds 12345 explicitly)
     * @param fileSystemName  backing image filename (e.g., "filesystem.dat")
     * @param totalSize       total image size in bytes (must be multiple of BLOCK_SIZE)
     */
    public FileServer(int port, String fileSystemName, int totalSize){
        // Instantiate the storage layer
        FileSystemManager fsManager = new FileSystemManager(fileSystemName, totalSize);
        this.fsManager = fsManager;
        this.port = port;
    }

    /**
     * Start a blocking accept loop on a ServerSocket.
     * Current behavior: binds specifically to 12345 (not the 'port' field).
     * For the assignment later, replace `new ServerSocket(12345)` with `new ServerSocket(port)`.
     */
    public void start(){
        try (ServerSocket serverSocket = new ServerSocket(12345)) {
            System.out.println("Server started. Listening on port 12345...");

            // Outer loop: accept client connections sequentially
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Handling client: " + clientSocket);

                // Auto-close streams when done with this client
                try (
                        BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                        PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true)
                ) {
                    String line;

                    // Inner loop: read one line at a time and dispatch
                    while ((line = reader.readLine()) != null) {
                        System.out.println("Received from client: " + line);

                        // For WRITE payloads, we need to preserve spaces -> split into at most 3 parts
                        String[] parts = line.split(" ", 3);
                        String command = parts[0].toUpperCase();

                        switch (command) {
                            // Added a ping command to check if the server is running and if the singlethreading is working correctly
                            case "PING": {
                                writer.println("PONG");
                                break;
                            }

                            case "CREATE": {
                                if (parts.length < 2) {
                                    writer.println("ERROR: missing filename");
                                    break;
                                }
                                String name = parts[1];
                                try {
                                    fsManager.createFile(name);
                                    writer.println("SUCCESS: File '" + name + "' created.");
                                    writer.flush();
                                } catch (Exception e) {
                                    // Pass through meaningful error message; fall back to generic
                                    writer.println(e.getMessage() != null ? e.getMessage() : "ERROR: internal error");
                                }
                                break;
                            }

                            case "WRITE": {
                                if (parts.length < 2) {
                                    writer.println("ERROR: missing filename");
                                    break;
                                }
                                String name = parts[1];
                                String b64 = (parts.length >= 3) ? parts[2] : "";
                                try {
                                    // Decode base64 payload (empty allowed)
                                    byte[] content = b64.isEmpty() ? new byte[0] : Base64.getDecoder().decode(b64);
                                    fsManager.writeFile(name, content);
                                    writer.println("OK");
                                } catch (IllegalArgumentException iae) {
                                    // Base64 decode error or invalid input: normalize into an ERROR line
                                    String msg = iae.getMessage();
                                    if (msg == null || msg.isBlank()) msg = "ERROR: invalid input";
                                    writer.println(msg.startsWith("ERROR:") ? msg : "ERROR: " + msg);
                                } catch (Exception e) {
                                    writer.println(e.getMessage() != null ? e.getMessage() : "ERROR: internal error");
                                }
                                break;
                            }

                            case "READ": {
                                if (parts.length < 2) {
                                    writer.println("ERROR: missing filename");
                                    break;
                                }
                                String name = parts[1];
                                try {
                                    byte[] data = fsManager.readFile(name);
                                    // Return base64-encoded content on a single line
                                    String b64 = Base64.getEncoder().encodeToString(data);
                                    writer.println(b64);
                                } catch (Exception e) {
                                    writer.println(e.getMessage() != null ? e.getMessage() : "ERROR: internal error");
                                }
                                break;
                            }

                            case "DELETE": {
                                if (parts.length < 2) {
                                    writer.println("ERROR: missing filename");
                                    break;
                                }
                                String name = parts[1];
                                try {
                                    fsManager.deleteFile(name);
                                    writer.println("OK");
                                } catch (Exception e) {
                                    writer.println(e.getMessage() != null ? e.getMessage() : "ERROR: internal error");
                                }
                                break;
                            }

                            case "LIST": {
                                try {
                                    String[] names = fsManager.listFiles();
                                    // Comma-separated list of filenames (empty line if none)
                                    writer.println(String.join(",", names));
                                } catch (Exception e) {
                                    writer.println(e.getMessage() != null ? e.getMessage() : "ERROR: internal error");
                                }
                                break;
                            }

                            case "QUIT":
                                // Current behavior: disconnect client AND return out of start() (stop server).
                                // For multi-client support later, you’ll want to just break out of this client’s loop
                                // and continue the accept loop instead of returning.
                                writer.println("SUCCESS: Disconnecting.");
                                return;

                            default:
                                writer.println("ERROR: Unknown command.");
                                break;
                        }
                    }
                } catch (Exception e) {
                    // Per-client exception: log and continue serving future clients
                    e.printStackTrace();
                } finally {
                    try { clientSocket.close(); } catch (Exception ignored) {}
                }
            }
        } catch (Exception e) {
            // Fatal server error (e.g., can't bind port)
            e.printStackTrace();
            System.err.println("Could not start server on port " + port);
        }
    }
}
