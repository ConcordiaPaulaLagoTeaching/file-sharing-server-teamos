package ca.concordia.server;

import ca.concordia.filesystem.FileSystemManager;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Arrays;


/*
 * ClientHandler class implements the Runnable interface to handle client connections in a multi-threaded environment.
 * It handles a single client, reads commands from the client and dispatches them to the appropriate method in the FileSystemManager.
 */
public class ClientHandler implements Runnable {
    private Socket clientSocket;
    private FileSystemManager fsManager;

    // Constructor for ClientHandler class
    public ClientHandler(Socket socket, FileSystemManager fsManager) {
        this.clientSocket = socket;
        this.fsManager = fsManager;
    }

    // Override the run method of the Runnable interface to handle client connections
    @Override
    public void run() {
        System.out.println("Handling client: " + clientSocket);
        
        try (
            BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true)
        ) {
            String line;

            // Read one line at a time and dispatch client to appropriate method in FileSystemManager
            while ((line = reader.readLine()) != null) {
                System.out.println("Received from client " + clientSocket + ": " + line);
                String[] parts = line.split(" ", 3); // Split into max 3 parts
                String command = parts[0].toUpperCase();

                switch (command) {

                    // Added a ping command to check if the server is running
                    case "PING": {
                        writer.println("PONG");
                        break;
                    }

                    // Create a new file
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
                            writer.println(e.getMessage() != null ? e.getMessage() : "ERROR: internal error");
                        }
                        break;
                    }

                    // Delete an existing file
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

                    // Write to an existing file
                    case "WRITE": {
                        if (parts.length < 2) {
                            writer.println("ERROR: missing filename");
                            break;
                        }
                        String name = parts[1];
                        String payload = (parts.length >= 3) ? parts[2] : "";
                        try {
                            byte[] contentBytes;
                            if (payload.isEmpty()) {
                                contentBytes = new byte[0];
                            } else {
                                try {
                                    contentBytes = Base64.getDecoder().decode(payload);
                                } catch (IllegalArgumentException ex) {
                                    contentBytes = payload.getBytes(StandardCharsets.UTF_8);
                                }
                            }
                            System.out.println("Content bytes: " + new String(contentBytes, StandardCharsets.UTF_8));
                            System.out.println("Payload: " + payload);
                            fsManager.writeFile(name, contentBytes);
                            writer.println("OK");
                        } catch (Exception e) {
                            writer.println(e.getMessage() != null ? e.getMessage() : "ERROR: internal error");
                        }
                        break;
                    }

                    // Read the contents of an existing file
                    case "READ": {
                        if (parts.length < 2) {
                            writer.println("ERROR: missing filename");
                            break;
                        }
                        String name = parts[1];
                        try {
                            byte[] data = fsManager.readFile(name);
                            String utf8;
                            try {
                                utf8 = new String(data, StandardCharsets.UTF_8);
                                if (Arrays.equals(utf8.getBytes(StandardCharsets.UTF_8), data)) {
                                    writer.println(utf8);
                                    break;
                                }
                            } catch (Exception ignore) {}
                            String b64 = Base64.getEncoder().encodeToString(data);
                            writer.println(b64);
                        } catch (Exception e) {
                            writer.println(e.getMessage() != null ? e.getMessage() : "ERROR: internal error");
                        }
                        break;
                    }

                    // List all files in the filesystem
                    case "LIST": {
                        try {
                            String[] names = fsManager.listFiles();
                            // Comma-separated list of filenames (empty line if none)
                            writer.println(String.join(", ", names));
                        } catch (Exception e) {
                            writer.println(e.getMessage() != null ? e.getMessage() : "ERROR: internal error");
                        }
                        break;
                    }

                    case "DEBUG": {
                        try {
                            fsManager.debugPrintFileSystem();
                            writer.println("SUCCESS: Debug information printed.");
                        } catch (Exception e) {
                            writer.println(e.getMessage() != null ? e.getMessage() : "ERROR: internal error");
                        }
                        break;
                    }
                    
                    case "QUIT":
                        // Disconnect each client only (not the entire server)
                        writer.println("SUCCESS: Disconnecting.");
                        return;

                    default:
                        writer.println("ERROR: Unknown command.");
                        break;
                }
            }
        } catch (Exception e) {
            System.err.println("Error handling client " + clientSocket + ": " + e.getMessage());
            e.printStackTrace();
        } finally {
            try { 
                clientSocket.close();
                System.out.println("Client disconnected: " + clientSocket);
            } catch (Exception e) {
                // Ignore
            }
        }
    }
}
