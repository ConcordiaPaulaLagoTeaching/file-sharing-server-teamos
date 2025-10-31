package ca.concordia.server;

import ca.concordia.filesystem.FileSystemManager;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Base64;

public class FileServer {

    private FileSystemManager fsManager;
    private int port;

    public FileServer(int port, String fileSystemName, int totalSize){
        FileSystemManager fsManager = new FileSystemManager(fileSystemName, totalSize);
        this.fsManager = fsManager;
        this.port = port;
    }

    public void start(){
        try (ServerSocket serverSocket = new ServerSocket(12345)) {
            System.out.println("Server started. Listening on port 12345...");

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Handling client: " + clientSocket);
                try (
                        BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                        PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true)
                ) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        System.out.println("Received from client: " + line);

                        String[] parts = line.split(" ", 3);
                        String command = parts[0].toUpperCase();

                        switch (command) {
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

                            case "WRITE": {
                                if (parts.length < 2) {
                                    writer.println("ERROR: missing filename");
                                    break;
                                }
                                String name = parts[1];
                                String b64 = (parts.length >= 3) ? parts[2] : "";
                                try {
                                    byte[] content = b64.isEmpty() ? new byte[0] : Base64.getDecoder().decode(b64);
                                    fsManager.writeFile(name, content);
                                    writer.println("OK");
                                } catch (IllegalArgumentException iae) {
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
                                    writer.println(String.join(",", names));
                                } catch (Exception e) {
                                    writer.println(e.getMessage() != null ? e.getMessage() : "ERROR: internal error");
                                }
                                break;
                            }

                            case "QUIT":
                                writer.println("SUCCESS: Disconnecting.");
                                return;

                            default:
                                writer.println("ERROR: Unknown command.");
                                break;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try { clientSocket.close(); } catch (Exception ignored) {}
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Could not start server on port " + port);
        }
    }
}
