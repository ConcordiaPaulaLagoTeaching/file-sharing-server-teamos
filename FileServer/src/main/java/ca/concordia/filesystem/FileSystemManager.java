package ca.concordia.filesystem;

import ca.concordia.filesystem.datastructures.FEntry;
import ca.concordia.filesystem.datastructures.FNode;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
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

    // -----------------------
    // Tunables / capacities
    // -----------------------
    private static final int BLOCK_SIZE = 128;   // Size of one data block (bytes)
    private static final int MAX_FILES  = 5;     // Max number of directory entries
    private static final int MAX_FNODES = 10;     // Max number of directory entries
    private final int MAX_BLOCKS;         // Number of data blocks (derived from total image size)

    // -----------------------
    // On-disk struct sizes
    // -----------------------
    private static final int FENTRY_NAME_LEN = 11;         // name bytes (ASCII)
    private static final int FENTRY_BYTES    = 11 + 2 + 2; // name[11] + size(u16) + firstFNode(i16)
    private static final int FNODE_BYTES     = 4 + 4;      // blockIndex(i32 absolute) + nextFNode(i32)

    // -----------------------
    // Disk layout info
    // -----------------------
    private final RandomAccessFile disk;   // backing file (the whole filesystem)

    private static final int FENTRY_TABLE_OFFSET = 0; 
    private static final int FNODE_TABLE_OFFSET  = FENTRY_TABLE_OFFSET + MAX_FILES * FENTRY_BYTES;

    private final int  metaBytes;          // total bytes of metadata region (entries + nodes)
    private final int  metaBlocks;         // metadata rounded up to whole blocks
    private final int  dataStartBlock;     // first absolute block index used for data
    private final long imageBytes;         // full file size (metadata blocks + data blocks) * BLOCK_SIZE

    // -----------------------
    // In-memory mirrors
    // -----------------------
    private final FEntry[] fEntryTable;
    private final FNode[]  fNodeTable;

    // Bitmaps for quick "free" queries (rebuilt on load and after mutations)
    private final BitSet usedDataBlocks;   // true if data-region block is referenced by a fnode
    private final BitSet usedFnodes;       // true if fnode[i] is in use (blockIndex >= 0)

    // Readers-writer lock: allows concurrent reads; writes are exclusive
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock(true); // flag set to true for fairness to prevent starvation


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

    /** Read an FNode from the disk. */
    private FNode readFNode(int index) throws IOException {
        // Seek to the start of the FNode at the given index
        disk.seek(FNODE_TABLE_OFFSET + index * FNODE_BYTES);

        // Read the block index and next block
        int blockIndex = disk.readInt();
        int nextBlock = disk.readInt();

        return new FNode(blockIndex, nextBlock);
    }

    /**
     * Mounts or initializes the filesystem image.
     * If the underlying file is new (length == 0), we initialize empty tables and zero data region.
     */
    public FileSystemManager(String filename, int totalSizeBytes) {
        try {
            // Validate total size: must be a positive multiple of block size
            if (totalSizeBytes <= 0 || (totalSizeBytes % BLOCK_SIZE) != 0) {
                throw new IllegalArgumentException("totalSize must be a positive multiple of BLOCK_SIZE");
            }
            this.MAX_BLOCKS = totalSizeBytes / BLOCK_SIZE;

            // Compute metadata footprint and layout offsets
            int feBytes = MAX_FILES * FENTRY_BYTES;
            int fnBytes = MAX_BLOCKS * FNODE_BYTES;
            this.metaBytes = feBytes + fnBytes;
            this.metaBlocks = (int) Math.ceil(metaBytes / (double) BLOCK_SIZE);
            this.dataStartBlock    = metaBlocks;

            // Total image length (metadata blocks + data blocks)
            this.imageBytes = (long) (metaBlocks + MAX_BLOCKS) * BLOCK_SIZE;

            // Open / size the file
            this.disk = new RandomAccessFile(filename, "rw");

            this.fEntryTable = new FEntry[MAX_FILES];
            this.fNodeTable  = new FNode[MAX_FNODES];

            // If disk is empty, read existing tables, otherwise create empty tables and zero data region
            if (disk.length() > 0) {
                // Read the FEntry table from disk
                for (int i = 0; i < fEntryTable.length; i++) {
                    fEntryTable[i] = readFEntry(i);
                }
                // Read the FNode table from disk
                for (int i = 0; i < fNodeTable.length; i++) {
                    fNodeTable[i] = readFNode(i);
                }
            }
            else {
                for (int i = 0; i < MAX_FILES; i++) fEntryTable[i] = new FEntry("", (short) 0, (short) -1);
                for (int i = 0; i < MAX_BLOCKS; i++) { fNodeTable[i] = new FNode(-1); fNodeTable[i].next = -1; }
                writeFEntries();
                writeFNodes();

                // Zero data region (each block) once
                byte[] zero = new byte[BLOCK_SIZE];
                for (int b = 0; b < MAX_BLOCKS; b++) {
                    disk.seek(blockOffset(dataStartBlock + b));
                    disk.write(zero);
                }
            }

            if (disk.length() < imageBytes) {
                disk.setLength(imageBytes); // grow to full image size
            }

            // Build bitmaps from loaded tables
            this.usedDataBlocks = new BitSet(MAX_BLOCKS);
            this.usedFnodes     = new BitSet(MAX_BLOCKS);
            rebuildBitmaps();

        } catch (IOException ioe) {
            throw new RuntimeException("Failed to open filesystem image", ioe);
        }
    }

    // =========================================================================
    // Public API (used by the server; these implement assignment semantics)
    // =========================================================================

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
            if (findEntry(fileName) >= 0) {
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

            // Create a new FEntry for the file at the free index
            fEntryTable[freeFentryIndex] = new FEntry(fileName, (short) 0, (short) -1);

            // Write the FEntry table to disk
            writeFEntries();
        }
        finally {
            // Releasing write lock after file creation is complete
            readWriteLock.writeLock().unlock();
        }
    }

    /**
     * Delete a file: free all fnodes in its chain and zero all data blocks for privacy.
     * Leaves the FEntry slot empty.
     */
    public void deleteFile(String fileName) throws Exception {
        // Acquire write lock.
        // This thread gets exclusive access to the filesystem until it releases the lock
        readWriteLock.writeLock().lock();
        try {
            // Find the index of the file in the FEntry table and throw an error if it doesn't exist
            int fentryIndex = findEntry(fileName);
            if (fentryIndex < 0) {
                throw new IllegalStateException("ERROR: file " + fileName + " does not exist");
            }

            // Get the FEntry for the file
            FEntry fileEntry = fEntryTable[fentryIndex];

            // Free chain & zero the data it referenced
            freeChainZero(fileEntry.getFirstBlock());

            // Free the FEntry slot
            fEntryTable[fentryIndex] = new FEntry("", (short) 0, (short) -1);

            // Write both FEntry and FNode tables to disk
            writeFEntries();
            writeFNodes();
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
            int ei = findEntry(fileName);
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
            int blocksNeeded = (size + BLOCK_SIZE - 1) / BLOCK_SIZE;

            // Collect free resources WITHOUT mutating any state
            List<Integer> freeData = collectFreeDataBlocks(blocksNeeded);
            List<Integer> freeFn   = collectFreeFnodes(blocksNeeded);
            if (blocksNeeded > 0 && (freeData.size() < blocksNeeded || freeFn.size() < blocksNeeded)) {
                throw new IllegalStateException("ERROR: file too large");
            }

            // Stage 1: write payload to selected data blocks
            int pos = 0;
            for (int k = 0; k < blocksNeeded; k++) {
                int dataBlockIdx = dataStartBlock + freeData.get(k); // absolute block index
                long off = blockOffset(dataBlockIdx);
                byte[] buf = new byte[BLOCK_SIZE];
                int len = Math.min(BLOCK_SIZE, size - pos);
                if (len > 0) System.arraycopy(contents, pos, buf, 0, len);
                disk.seek(off);
                disk.write(buf);
                pos += len;
            }

            // Stage 2: build new fnode chain (not yet reachable via directory)
            Integer head = null, prev = null;
            for (int k = 0; k < blocksNeeded; k++) {
                int fnIdx = freeFn.get(k);
                int dataBlockAbs = dataStartBlock + freeData.get(k);
                fNodeTable[fnIdx] = new FNode(dataBlockAbs);
                fNodeTable[fnIdx].next = -1;
                if (prev != null) fNodeTable[prev].next = fnIdx;
                if (head == null) head = fnIdx;
                prev = fnIdx;
            }
            writeFNodes(); // persist new chain

            // Stage 3: atomically "flip" directory entry to new chain
            short newSizeU16 = (short) (size & 0xFFFF);
            short newHeadS16 = (short) ((head == null) ? -1 : head.intValue());
            fEntryTable[ei].setFilesize(newSizeU16);
            // FEntry has no setter for firstBlock → re-create entry with same name
            fEntryTable[ei] = new FEntry(fEntryTable[ei].getFilename(), newSizeU16, newHeadS16);
            writeFEntries();

            // Stage 4: free + zero OLD chain (do AFTER flipping so write is atomic)
            if (old.getFirstBlock() >= 0) {
                freeChainZero(old.getFirstBlock());
                writeFNodes();
            }

            // Recompute used bitmaps for future allocations
            rebuildBitmaps();

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
            int fentryIndex = findEntry(fileName);
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
                long off = blockOffset(n.blockIndex);
                byte[] buf = new byte[Math.min(BLOCK_SIZE, remaining)];
                disk.seek(off);
                disk.readFully(buf, 0, buf.length);
                chunks.add(buf);
                remaining -= buf.length;
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

    // =========================================================================
    // Helpers: lookup, allocation, freeing, serialization
    // =========================================================================

    /** Linear search for filename in fentries (small MAX_FILES, so OK). */
    private int findEntry(String filename) {
        for (int i = 0; i < fEntryTable.length; i++) {
            FEntry e = fEntryTable[i];
            if (e != null && !e.getFilename().isEmpty() && e.getFilename().equals(filename)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Free a chain starting at head index: zero its data blocks, mark fnodes free.
     * Safe if head == -1 (no-op).
     */
    private void freeChainZero(int head) throws IOException {
        int fi = head;
        while (fi != -1) {
            FNode n = fNodeTable[fi];
            if (n == null) break;

            // Overwrite data for privacy
            if (n.blockIndex >= 0) {
                long off = blockOffset(n.blockIndex);
                byte[] zero = new byte[BLOCK_SIZE];
                disk.seek(off);
                disk.write(zero);
            }

            // Free node (mark unused)
            fNodeTable[fi].blockIndex = -1;
            int next = fNodeTable[fi].next;
            fNodeTable[fi].next = -1;
            fi = next;
        }
        rebuildBitmaps();
    }

    /** Collect N free data-region block indices (relative to data region), up to 'needed'. */
    private List<Integer> collectFreeDataBlocks(int needed) {
        List<Integer> out = new ArrayList<>(needed);
        if (needed == 0) return out;
        for (int i = 0; i < MAX_BLOCKS && out.size() < needed; i++) {
            if (!usedDataBlocks.get(i)) out.add(i);
        }
        return out;
    }

    /** Collect N free fnode indices, up to 'needed'. */
    private List<Integer> collectFreeFnodes(int needed) {
        List<Integer> out = new ArrayList<>(needed);
        if (needed == 0) return out;
        for (int i = 0; i < MAX_BLOCKS && out.size() < needed; i++) {
            if (!usedFnodes.get(i)) out.add(i);
        }
        return out;
    }

    /** Re-scan fnodes to rebuild the "used" bitmaps (called after changes). */
    private void rebuildBitmaps() {
        usedDataBlocks.clear();
        usedFnodes.clear();
        for (int i = 0; i < MAX_BLOCKS; i++) {
            FNode n = fNodeTable[i];
            if (n != null && n.blockIndex >= 0) {
                usedFnodes.set(i);
                int dataRegionIdx = n.blockIndex - dataStartBlock;
                if (dataRegionIdx >= 0 && dataRegionIdx < MAX_BLOCKS) {
                    usedDataBlocks.set(dataRegionIdx);
                }
            }
        }
    }

    /** Convert absolute block index to byte offset in the file. */
    private long blockOffset(int absoluteBlockIndex) {
        return (long) absoluteBlockIndex * BLOCK_SIZE;
    }

    // -----------------------
    // Serialization helpers
    // -----------------------

    /** Persist all FEntries in order (fixed width). */
    private void writeFEntries() throws IOException {
        long off = FENTRY_TABLE_OFFSET;
        disk.seek(off);
        for (int i = 0; i < MAX_FILES; i++) {
            FEntry e = fEntryTable[i];

            // name[11] (ASCII, padded with zeros)
            byte[] nameBytes = new byte[FENTRY_NAME_LEN];
            if (e != null && !e.getFilename().isEmpty()) {
                byte[] raw = e.getFilename().getBytes(StandardCharsets.UTF_8);
                System.arraycopy(raw, 0, nameBytes, 0, Math.min(raw.length, FENTRY_NAME_LEN));
            }
            disk.write(nameBytes);

            // size (u16) and first (i16)
            short size = (e == null) ? 0 : e.getFilesize();
            short fst  = (e == null) ? -1 : e.getFirstBlock();
            disk.writeShort(size);
            disk.writeShort(fst);
        }
    }

    /** Persist all FNodes in order. */
    private void writeFNodes() throws IOException {
        long off = FNODE_TABLE_OFFSET;
        disk.seek(off);
        for (int i = 0; i < MAX_BLOCKS; i++) {
            FNode n = fNodeTable[i];
            int bi = (n == null) ? -1 : n.blockIndex;
            int nx = (n == null) ? -1 : n.next;
            disk.writeInt(bi);
            disk.writeInt(nx);
        }
    }
}
