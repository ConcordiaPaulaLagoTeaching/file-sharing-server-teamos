package ca.concordia.server;

import ca.concordia.filesystem.FileSystemManager;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;


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
        System.out.println("Handling client: " + clientSocket + " in thread " + Thread.currentThread().getName());
        
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
                        String content = (parts.length >= 3) ? parts[2] : "";
                        try {
                            // Convert plain text to bytes
                            byte[] contentBytes = content.getBytes("UTF-8");
                            fsManager.writeFile(name, contentBytes);
                            writer.println("SUCCESS: Written to file '" + name + "'");
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
                            String content = new String(data, "UTF-8");
                            writer.println(content);
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
