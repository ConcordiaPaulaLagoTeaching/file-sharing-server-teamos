package ca.concordia;

import ca.concordia.server.FileServer;

public class Main {
    public static void main(String[] args) {
        System.out.printf("Hello and welcome!");

        FileServer server = new FileServer(12345, "filesystem.dat", 10 * 128);
        // Start the file server
        server.start();
    }
}