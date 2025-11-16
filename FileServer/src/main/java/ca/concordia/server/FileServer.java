package ca.concordia.server;

import ca.concordia.filesystem.FileSystemManager;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/*
 * FileServer class is the main class that starts the server and listens for client connections.
 * It creates a FileSystemManager instance and uses a virtual thread executor to handle client connections.
 * It accepts clients, creates a ClientHandler instance for each client connection,
 * and submits it to the virtual thread executor to handle the client.
 */
public class FileServer {
    private FileSystemManager fsManager;
    private int port;

    // Constructor for FileServer class
    public FileServer(int port, String fileSystemName, int totalSize){
        this.fsManager = new FileSystemManager(fileSystemName, totalSize);
        this.port = port;
    }

    // Start the server and listen for client connections
    public void start(){
        // Create a virtual thread executor to run each client connection in a separate virtual thread
        // Virtual thread is implemented to handle millions of concurrent tasks efficiently
        try (ExecutorService virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
             ServerSocket serverSocket = new ServerSocket(this.port)) { // Create a server socket to listen for client connections
            
            System.out.println("Server started. Listening on port " + this.port + "...");

            while (true) {
                try {
                    // Accept a client connection
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("New client connected: " + clientSocket);

                    // Submit client handler to virtual thread executor
                    ClientHandler clientHandler = new ClientHandler(clientSocket, fsManager);
                    virtualThreadExecutor.submit(clientHandler);
                } catch (Exception e) {
                    // Print the error message but continue accepting other clients
                    System.err.println("Error accepting/handling client connection: " + e.getMessage());
                    e.printStackTrace();
                }
            } 
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Server error: " + e.getMessage());
        }
    }
}
