package ca.concordia.filesystem;

import ca.concordia.filesystem.datastructures.FEntry;
import ca.concordia.filesystem.datastructures.FNode;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.*;

/**
 * FileSystemManager is a tiny, persistent, single-file filesystem with fixed-size metadata:
 * - The entire filesystem lives in one file (RandomAccessFile).
 * - Metadata region: array of FEntry (directory) + array of FNode (block pointers).
 * - Data region     : fixed number of data blocks, each BLOCK_SIZE bytes.
 *
 * Key operations (assignment requirements):
 *  - createFile(name)
 *  - writeFile(name, bytes)  -> transactional: all-or-nothing
 *  - readFile(name)          -> reads exactly the stored size
 *  - deleteFile(name)        -> frees chain and zeroes data for privacy
 *  - listFiles()
 *
 * Concurrency:
 *  - ReentrantReadWriteLock: concurrent READs; exclusive for CREATE/WRITE/DELETE.
 */
public class FileSystemManager {

    // Constants for the filesystem
    private static final int BLOCKSIZE = 128; // Size of one data block in bytes
    private static final int MAXFILES  = 5;   // Max number of files (FEntry entries)
    private static final int MAXBLOCKS = 10;  // Max number of data blocks (FNode entries)

    private static final int FENTRY_NAME_LEN = 11;         // Max length of a filename in bytes
    private static final int FENTRY_BYTES    = 11 + 2 + 2; // Size of an FEntry in bytes: name (11) + size (2) + firstFNode (2)
    private static final int FNODE_BYTES     = 4 + 4;      // Size of an FNode in bytes: blockIndex (4) + nextFNode (4)
    
    private static final int FENTRY_TABLE_OFFSET  = 0; 
    private static final int FNODE_TABLE_OFFSET   = FENTRY_TABLE_OFFSET + MAXFILES * FENTRY_BYTES;
    private static final int TOTAL_METADATA_SIZE  = FNODE_TABLE_OFFSET + MAXBLOCKS * FNODE_BYTES;
    private static final int METADATA_BLOCK_COUNT = (TOTAL_METADATA_SIZE + BLOCKSIZE - 1) / BLOCKSIZE; // round up TOTAL_METADATA_SIZE / BLOCK_SIZE to the nearest integer
    private static final int DATA_REGION_OFFSET   = METADATA_BLOCK_COUNT * BLOCKSIZE;
    
    // In-memory mirrors of the filesystem
    private final FEntry[] fEntryTable; // Array of FEntry entries
    private final FNode[]  fNodeTable;  // Array of FNode entries
    
    // Backing file for filesystem
    private final RandomAccessFile disk; 
    // Readers-writer lock: allows concurrent reads; writes are exclusive
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);

    private static FileSystemManager instance = null;

    /** Read an FEntry from the disk. */
    private FEntry readFEntry(int index) throws IOException {
        // Seek to the start of the FEntry at the given index
        disk.seek(FENTRY_TABLE_OFFSET + index * FENTRY_BYTES);

        // Read the filename
        byte[] nameBytes = new byte[FENTRY_NAME_LEN];
        disk.readFully(nameBytes);
        String fileName = new String(nameBytes, StandardCharsets.UTF_8).trim();

        // Read the file size and first block
        short fileSize = disk.readShort();
        short firstBlock = disk.readShort();

        return new FEntry(fileName, fileSize, firstBlock);
    }
    
    /** Write an FEntry to the disk. */
    private void writeFEntry(int index, FEntry fEntry) throws IOException {
        // Seek to the start of the FEntry at the given index
        disk.seek(FENTRY_TABLE_OFFSET + index * FENTRY_BYTES);

        // Ensure filename is exactly 11 bytes (pad with zeros or truncate)
        byte[] nameBytes = fEntry.getFilename().getBytes(StandardCharsets.UTF_8);
        byte[] paddedName = java.util.Arrays.copyOf(nameBytes, FENTRY_NAME_LEN);
        
        // Write fEntry content to disk
        disk.write(paddedName);
        disk.writeShort(fEntry.getFilesize());
        disk.writeShort(fEntry.getFirstBlock());

        // Finally, update the in-memory mirror if everything went well
        fEntryTable[index] = fEntry;
    }

    /** Read an FNode from the disk. */
    private FNode readFNode(int index) throws IOException {
        // Seek to the start of the FNode at the given index
        disk.seek(FNODE_TABLE_OFFSET + index * FNODE_BYTES);

        // Read the block index and next block
        int blockIndex = disk.readInt();
        int nextBlock = disk.readInt();

        return new FNode(blockIndex, nextBlock);
    }

    /** Write an FNode to the disk. */
    private void writeFNode(int index, FNode fNode) throws IOException {
        // Seek to the start of the FNode at the given index
        disk.seek(FNODE_TABLE_OFFSET + index * FNODE_BYTES);

        // Write fNode content to disk
        disk.writeInt(fNode.blockIndex);
        disk.writeInt(fNode.next);

        // Finally, update the in-memory mirror if everything went well
        fNodeTable[index] = fNode;
    }

    /** Get the offset of the data block at the given index. */
    private int getDataBlockOffset(int index) {
        return DATA_REGION_OFFSET + index * BLOCKSIZE;
    }

    /** Write the data to the disk at the given index. */
    private void writeDataBlock(int index, byte[] data) throws IOException {
        // Seek to the start of the data block at the given index
        disk.seek(getDataBlockOffset(index));

        // Write the data to the disk
        disk.write(data);
    }

    /** Read the data from the disk at the given index. */
    private byte[] readDataBlock(int index, int length) throws IOException {
        // Seek to the start of the data block at the given index
        disk.seek(getDataBlockOffset(index));

        // Read the data from the disk
        byte[] data = new byte[length];
        disk.readFully(data);
        return data;
    }

    /** Find the index of the FEntry with the given filename. Return -1 if not found. */
    private int findFEntry(String filename) {
        for (int i = 0; i < fEntryTable.length; i++) {
            FEntry e = fEntryTable[i];
            if (e != null && !e.getFilename().isEmpty() && e.getFilename().equals(filename)) {
                return i;
            }
        }
        return -1;
    }

    /** Get the first N free FNodes. */
    private List<Integer> collectFreeFNodes(int needed) {
        List<Integer> out = new ArrayList<>(needed);
        if (needed == 0) return out;
        for (int i = 0; i < MAXBLOCKS && out.size() < needed; i++) {
            if (fNodeTable[i].blockIndex < 0) out.add(i);
        }
        return out;
    }

    /** Free an FNode chain starting at the given index and zeroing the data blocks. */
    private void freeFNode(int index) throws IOException {
        // Simpy return if the index is out of bounds
        if (index < 0 || index >= MAXBLOCKS) {
            return;
        }
        // No need to free if the FNode is already free
        if (fNodeTable[index].blockIndex < 0) {
            return;
        }

        int nextIndex = fNodeTable[index].next;

        // Write 0s to the corresponding data block
        writeDataBlock(fNodeTable[index].blockIndex, new byte[BLOCKSIZE]);

        // Free the FNode by writing an empty FNode to the disk
        writeFNode(index, new FNode(-index, -1));
        
        // Free the next FNode
        freeFNode(nextIndex);
    }

    public void debugPrintFileSystem() {
        System.out.println("Constants for the filesystem:");
        System.out.println("BLOCKSIZE: " + BLOCKSIZE);
        System.out.println("MAXFILES: " + MAXFILES);
        System.out.println("MAXBLOCKS: " + MAXBLOCKS);
        System.out.println();
        System.out.println("FENTRY_NAME_LEN: " + FENTRY_NAME_LEN);
        System.out.println("FENTRY_BYTES: " + FENTRY_BYTES);
        System.out.println("FNODE_BYTES: " + FNODE_BYTES);
        System.out.println();
        System.out.println("FENTRY_TABLE_OFFSET: " + FENTRY_TABLE_OFFSET);
        System.out.println("FNODE_TABLE_OFFSET: " + FNODE_TABLE_OFFSET);
        System.out.println("TOTAL_METADATA_SIZE: " + TOTAL_METADATA_SIZE);
        System.out.println("METADATA_BLOCK_COUNT: " + METADATA_BLOCK_COUNT);
        System.out.println("DATA_REGION_OFFSET: " + DATA_REGION_OFFSET);
        System.out.println();

        System.out.println("fEntryTable:");
        for (int i = 0; i < fEntryTable.length; i++) {
            System.out.println(i + ": " + fEntryTable[i].getFilename() + ", " + fEntryTable[i].getFilesize() + ", " + fEntryTable[i].getFirstBlock());
        }
        System.out.println();
        System.out.println("fNodeTable:");
        for (int i = 0; i < fNodeTable.length; i++) {
            String data = "EMPTY";
            if (fNodeTable[i].blockIndex >= 0) {
                data = new String(readDataBlock(fNodeTable[i].blockIndex, BLOCKSIZE), StandardCharsets.UTF_8);
            }
            System.out.println(i + ": " + fNodeTable[i].blockIndex + ", " + fNodeTable[i].next + ", " + data);
        }
        System.out.println();
        System.out.flush();
    }

    /**
     * Mounts or initializes the filesystem image.
     * If the underlying file is new (length == 0), we initialize empty tables and zero data region.
     */
    public FileSystemManager(String filename, int totalSizeBytes) {
        // If the FileSystemManager is already initialized, throw an error. We only want one instance of the FileSystemManager.
        if (instance != null) {
            throw new IllegalStateException("FileSystemManager is already initialized.");
        }
        // Total size must be a positive multiple of block size
        if (totalSizeBytes <= 0 || (totalSizeBytes % BLOCKSIZE) != 0) {
            throw new IllegalArgumentException("Filesystem total size must be a positive multiple of BLOCK_SIZE");
        }

        try {
            this.disk = new RandomAccessFile(filename, "rw");

            this.fEntryTable = new FEntry[MAXFILES];
            this.fNodeTable  = new FNode[MAXBLOCKS];

            if (disk.length() > 0) { // If disk already exists, read data from the disk
                // Read the FEntry and FNode tables from disk
                for (int i = 0; i < fEntryTable.length; i++) {
                    fEntryTable[i] = readFEntry(i);
                }
                for (int i = 0; i < fNodeTable.length; i++) {
                    fNodeTable[i] = readFNode(i);
                }
            }
            else { // otherwise, create empty tables and zero data region
                for (int i = 0; i < fEntryTable.length; i++) {
                    writeFEntry(i, new FEntry("", (short) 0, (short) -1));
                }
                for (int i = 0; i < fNodeTable.length; i++) {
                    fNodeTable[i] = new FNode(-1, -1);
                    writeFNode(i, fNodeTable[i]);
                    // Write 0s to the corresponding data block
                    writeDataBlock(i, new byte[BLOCKSIZE]);
                }
            }

            instance = this; // Set the instance to the current FileSystemManager
        } catch (IOException ioe) {
            instance = null;
            throw new RuntimeException("Failed to open filesystem image", ioe);
        }
    }

    public void createFile(String fileName) throws Exception {
        if (fileName.isEmpty()) {
            throw new IllegalArgumentException("ERROR: filename cannot be empty");
        }
        if (fileName.length() > FENTRY_NAME_LEN) {
            throw new IllegalArgumentException("ERROR: filename too large");
        }

        // Acquire write lock.
        // This thread gets exclusive access to the filesystem until it releases the lock
        readWriteLock.writeLock().lock();
        try {
            // Check if file already exists
            if (findFEntry(fileName) >= 0) {
                throw new IllegalStateException("ERROR: file " + fileName + " already exists");
            }

            // Find the first free FEntry index
            int freeFentryIndex = -1;
            for (int i = 0; i < fEntryTable.length; i++) {
                if (fEntryTable[i] == null || fEntryTable[i].getFilename().isEmpty()) {
                    freeFentryIndex = i;
                    break;
                }
            }

            // If no free FEntry is found, throw an error
            if (freeFentryIndex < 0) {
                throw new IllegalStateException("ERROR: cannot create more files");
            }

            // Write the FEntry for the new file to disk
            writeFEntry(freeFentryIndex, new FEntry(fileName, (short) 0, (short) -1));
        }
        finally {
            // Releasing write lock after file creation is complete
            readWriteLock.writeLock().unlock();
        }
    }

    public void deleteFile(String fileName) throws Exception {
        // Acquire write lock.
        // This thread gets exclusive access to the filesystem until it releases the lock
        readWriteLock.writeLock().lock();
        try {
            // Find the index of the file in the FEntry table and throw an error if it doesn't exist
            int fentryIndex = findFEntry(fileName);
            if (fentryIndex < 0) {
                throw new IllegalStateException("ERROR: file " + fileName + " does not exist");
            }

            // Free the FNode chain & zero the data it referenced
            freeFNode(fEntryTable[fentryIndex].getFirstBlock());

            // Free the FEntry slot by writing a empty FEntry to the disk
            writeFEntry(fentryIndex, new FEntry("", (short) 0, (short) -1));
        }
        finally {
            // Releasing write lock after file deletion is complete
            readWriteLock.writeLock().unlock();
        }
    }

    /**
     * Transactional write:
     * 1) Validate resources (enough data blocks + fnodes).
     * 2) Write new data blocks.
     * 3) Build new fnode chain and persist it.
     * 4) Atomically flip FEntry to point to new chain (size + head).
     * 5) Free/zero the OLD chain.
     */
    public void writeFile(String fileName, byte[] contents) throws Exception {
        readWriteLock.writeLock().lock();
        try {
            // Find the index of the file in the FEntry table and throw an error if it doesn't exist
            int ei = findFEntry(fileName);
            if (ei < 0)  {
                throw new IllegalStateException("ERROR: file " + fileName + " does not exist");
            }

            // Get the current FEntry for the file
            FEntry old = fEntryTable[ei];

            // Size validation (fits in unsigned short per spec)
            int size = (contents == null) ? 0 : contents.length;
            if (size > 0xFFFF) {
                throw new IllegalStateException("ERROR: file too large");
            }

            // Compute number of blocks needed for payload
            int blocksNeeded = (size + BLOCKSIZE - 1) / BLOCKSIZE;

            List<Integer> freeFn   = collectFreeFNodes(blocksNeeded);
            if (blocksNeeded > 0 && (freeFn.size() < blocksNeeded)) {
                throw new IllegalStateException("ERROR: file too large");
            }

            // Stage 1: write payload to selected data blocks
            int pos = 0;
            for (int k = 0; k < blocksNeeded; k++) {
                int dataBlockIdx = METADATA_BLOCK_COUNT + freeFn.get(k); // absolute block index
                long off = getDataBlockOffset(dataBlockIdx);
                byte[] buf = new byte[BLOCKSIZE];
                int len = Math.min(BLOCKSIZE, size - pos);
                if (len > 0) System.arraycopy(contents, pos, buf, 0, len);
                disk.seek(off);
                disk.write(buf);
                pos += len;
            }

            // Stage 2: build new fnode chain (not yet reachable via directory)
            Integer head = null, prev = null;
            for (int k = 0; k < blocksNeeded; k++) {
                int fnIdx = freeFn.get(k);

                int dataBlockAbs = METADATA_BLOCK_COUNT + freeFn.get(k);
                fNodeTable[fnIdx] = new FNode(dataBlockAbs);
                fNodeTable[fnIdx].next = -1;

                if (prev != null) fNodeTable[prev].next = fnIdx;
                if (head == null) head = fnIdx;

                prev = fnIdx;
            }
            // Write the updated FNode chain to disk
            for (int k = 0; k < blocksNeeded; k++) {
                writeFNode(freeFn.get(k), fNodeTable[freeFn.get(k)]);
            }

            // Stage 3: atomically "flip" directory entry to new chain
            short newSizeU16 = (short) (size & 0xFFFF);
            short newHeadS16 = (short) ((head == null) ? -1 : head.intValue());
            writeFEntry(ei, new FEntry(fEntryTable[ei].getFilename(), newSizeU16, newHeadS16));

            // Stage 4: free + zero OLD chain (do AFTER flipping so write is atomic)
            if (old.getFirstBlock() >= 0) {
                freeFNode(old.getFirstBlock());
            }
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    /**
     * Read exactly the stored size from the chain into a byte[].
     * Errors if chain is broken or file is missing (consistent with spec).
     */
    public byte[] readFile(String fileName) throws Exception {
        readWriteLock.readLock().lock();
        try {
            // Find the index of the file in the FEntry table and throw an error if it doesn't exist
            int fentryIndex = findFEntry(fileName);
            if (fentryIndex < 0) {
                throw new IllegalStateException("ERROR: file " + fileName + " does not exist");
            }

            // Get the FEntry for the file
            FEntry e = fEntryTable[fentryIndex];
            int remaining = Short.toUnsignedInt(e.getFilesize());
            if (remaining == 0) return new byte[0];

            List<byte[]> chunks = new ArrayList<>();
            int fi = e.getFirstBlock();
            while (fi != -1 && remaining > 0) {
                FNode n = fNodeTable[fi];
                if (n == null || n.blockIndex < 0) {
                    // If a chain node is invalid, treat as missing/corrupt
                    throw new IllegalStateException("ERROR: file " + fileName + " does not exist");
                }

                byte[] data = readDataBlock(n.blockIndex, Math.min(BLOCKSIZE, remaining));
                chunks.add(data);
                remaining -= data.length;
                fi = n.next;
            }

            // Stitch chunks into a single byte array (exact file size)
            byte[] out = new byte[Short.toUnsignedInt(e.getFilesize())];
            int p = 0;
            for (byte[] c : chunks) { System.arraycopy(c, 0, out, p, c.length); p += c.length; }
            return out;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public String[] listFiles() {
        // Acquire read lock for getting shared read access to the filesystem
        readWriteLock.readLock().lock();
        try {
            ArrayList<String> fileNames = new ArrayList<>();
            for (int i = 0; i < fEntryTable.length; i++) {
                if (fEntryTable[i] != null && !fEntryTable[i].getFilename().isEmpty()) {
                    fileNames.add(fEntryTable[i].getFilename());
                }
            }
            fileNames.sort(String::compareTo); // Sort the filenames in ascending order

            return fileNames.toArray(new String[fileNames.size()]); // Convert the ArrayList to an array
        } finally {
            // Releasing read lock after read operation is complete
            readWriteLock.readLock().unlock();
        }
    }
}
