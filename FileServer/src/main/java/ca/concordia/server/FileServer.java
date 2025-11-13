package ca.concordia.server;

import ca.concordia.filesystem.FileSystemManager;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Base64;

class ClientHandler extends Thread {
    private Socket clientSocket;
    private FileSystemManager fsManager;

    public ClientHandler(Socket socket, FileSystemManager fsManager) {
        this.clientSocket = socket;
        this.fsManager = fsManager;
    }

    @Override
    public void run() {
        System.out.println("Handling client: " + clientSocket + " in thread " + Thread.currentThread().getName());
        
        // Auto-close streams when done with this client
        try (
                BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true)
        ) {
            String line;

            // Inner loop: read one line at a time and dispatch
            while ((line = reader.readLine()) != null) {
                System.out.println("Received from client " + clientSocket + ": " + line);

                // For WRITE payloads, we need to preserve spaces -> split into at most 3 parts
                String[] parts = line.split(" ", 3);
                String command = parts[0].toUpperCase();

                switch (command) {
                    // Added a ping command to check if the server is running and if the singlethreading is working correctly
                    case "PING": {
                        writer.println("PONG");
                        break;
                    }

                    case "QUIT":
                        // Disconnect this client only (not the entire server)
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
            } catch (Exception ignored) {}
        }
    }
}


public class FileServer {
    private FileSystemManager fsManager;
    private int port;

    public FileServer(int port, String fileSystemName, int totalSize){
        this.fsManager = new FileSystemManager(fileSystemName, totalSize);
        this.port = port;
    }

    public void start(){
        try (ServerSocket serverSocket = new ServerSocket(this.port)) {
            System.out.println("Server started. Listening on port " + this.port + "...");

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("New client connected: " + clientSocket);

                ClientHandler handler = new ClientHandler(clientSocket, fsManager);
                handler.start();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Could not start server on port " + port);
        }
    }
}
