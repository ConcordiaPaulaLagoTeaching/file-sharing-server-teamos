package ca.concordia.filesystem;

import ca.concordia.filesystem.datastructures.FEntry;
import ca.concordia.filesystem.datastructures.FNode;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
    private final int BLOCK_SIZE = 128;   // Size of one data block (bytes)
    private final int MAX_FILES  = 5;     // Max number of directory entries
    private final int MAX_BLOCKS;         // Number of data blocks (derived from total image size)

    // -----------------------
    // On-disk struct sizes
    // -----------------------
    private static final int FENTRY_NAME_LEN = 11;       // name bytes (ASCII)
    private static final int FENTRY_BYTES    = 11 + 2 + 2; // name[11] + size(u16) + firstFNode(i16)
    private static final int FNODE_BYTES     = 4 + 4;      // blockIndex(i32 absolute) + nextFNode(i32)

    // -----------------------
    // Disk layout info
    // -----------------------
    private final RandomAccessFile disk;   // backing file (the whole filesystem)
    private final long fentryTableOffset;  // start of FEntry table (always 0)
    private final long fnodeTableOffset;   // start of FNode table (after all FEntries)
    private final int  metaBytes;          // total bytes of metadata region (entries + nodes)
    private final int  metaBlocks;         // metadata rounded up to whole blocks
    private final int  dataStartBlock;     // first absolute block index used for data
    private final long imageBytes;         // full file size (metadata blocks + data blocks) * BLOCK_SIZE

    // -----------------------
    // In-memory mirrors
    // -----------------------
    private final FEntry[] fentries;       // directory entries
    private final FNode[]  fnodes;         // fnode array, each points to a data block and next fnode

    // Bitmaps for quick "free" queries (rebuilt on load and after mutations)
    private final BitSet usedDataBlocks;   // true if data-region block is referenced by a fnode
    private final BitSet usedFnodes;       // true if fnode[i] is in use (blockIndex >= 0)

    // Readers-writer lock: allows concurrent reads; writes are exclusive
    private final ReentrantReadWriteLock rw = new ReentrantReadWriteLock(true);

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
            this.fentryTableOffset = 0L;
            this.fnodeTableOffset  = fentryTableOffset + feBytes;
            this.dataStartBlock    = metaBlocks;

            // Total image length (metadata blocks + data blocks)
            this.imageBytes = (long) (metaBlocks + MAX_BLOCKS) * BLOCK_SIZE;

            // Open / size the file
            this.disk = new RandomAccessFile(filename, "rw");
            long oldLen = disk.length();
            if (disk.length() < imageBytes) {
                disk.setLength(imageBytes); // grow to full image size
            }

            // Prepare in-memory tables
            this.fentries = new FEntry[MAX_FILES];
            this.fnodes   = new FNode[MAX_BLOCKS];
            // If oldLen == 0 → brand new image; otherwise read existing tables
            loadOrInitTables(oldLen == 0);

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

    /**
     * Create an empty file with the given name (≤ 11 chars) using first free FEntry.
     * Errors:
     *  - "ERROR: filename too large"
     *  - "ERROR: file <name> already exists"
     *  - "ERROR: file too large" (used here to signal no free directory entry)
     */
    public void createFile(String fileName) throws Exception {
        validateName(fileName);
        rw.writeLock().lock();
        try {
            if (findEntry(fileName) >= 0) {
                throw new IllegalStateException("ERROR: file " + fileName + " already exists");
            }
            int slot = firstFreeFentry();
            if (slot < 0) {
                // Spec uses this message for "not enough resources" as well
                throw new IllegalStateException("ERROR: file too large");
            }
            fentries[slot] = new FEntry(fileName, (short) 0, (short) -1);
            writeFEntries(); // persist directory table
        } finally {
            rw.writeLock().unlock();
        }
    }

    /**
     * Delete a file: free all fnodes in its chain and zero all data blocks for privacy.
     * Leaves the FEntry slot empty.
     */
    public void deleteFile(String fileName) throws Exception {
        rw.writeLock().lock();
        try {
            int ei = findEntryOrThrow(fileName);
            FEntry e = fentries[ei];

            // Free chain & zero the data it referenced
            freeChainZero(e.getFirstBlock());

            // Mark directory slot empty and persist
            fentries[ei] = new FEntry("", (short) 0, (short) -1);
            writeFEntries();
            writeFNodes(); // persist node updates from free
        } finally {
            rw.writeLock().unlock();
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
        rw.writeLock().lock();
        try {
            int ei = findEntryOrThrow(fileName);
            FEntry old = fentries[ei];

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
                fnodes[fnIdx] = new FNode(dataBlockAbs);
                fnodes[fnIdx].next = -1;
                if (prev != null) fnodes[prev].next = fnIdx;
                if (head == null) head = fnIdx;
                prev = fnIdx;
            }
            writeFNodes(); // persist new chain

            // Stage 3: atomically "flip" directory entry to new chain
            short newSizeU16 = (short) (size & 0xFFFF);
            short newHeadS16 = (short) ((head == null) ? -1 : head.intValue());
            fentries[ei].setFilesize(newSizeU16);
            // FEntry has no setter for firstBlock → re-create entry with same name
            fentries[ei] = new FEntry(fentries[ei].getFilename(), newSizeU16, newHeadS16);
            writeFEntries();

            // Stage 4: free + zero OLD chain (do AFTER flipping so write is atomic)
            if (old.getFirstBlock() >= 0) {
                freeChainZero(old.getFirstBlock());
                writeFNodes();
            }

            // Recompute used bitmaps for future allocations
            rebuildBitmaps();

        } finally {
            rw.writeLock().unlock();
        }
    }

    /**
     * Read exactly the stored size from the chain into a byte[].
     * Errors if chain is broken or file is missing (consistent with spec).
     */
    public byte[] readFile(String fileName) throws Exception {
        rw.readLock().lock();
        try {
            int ei = findEntryOrThrow(fileName);
            FEntry e = fentries[ei];
            int remaining = Short.toUnsignedInt(e.getFilesize());
            if (remaining == 0) return new byte[0];

            List<byte[]> chunks = new ArrayList<>();
            int fi = e.getFirstBlock();
            while (fi != -1 && remaining > 0) {
                FNode n = fnodes[fi];
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
            rw.readLock().unlock();
        }
    }

    /**
     * Return all non-empty filenames (sorted).
     */
    public String[] listFiles() {
        rw.readLock().lock();
        try {
            return Arrays.stream(fentries)
                    .map(FS::nameOrEmpty)
                    .filter(n -> !n.isEmpty())
                    .sorted()
                    .toArray(String[]::new);
        } finally {
            rw.readLock().unlock();
        }
    }

    // =========================================================================
    // Helpers: lookup, allocation, freeing, serialization
    // =========================================================================

    /** Validate filename constraints per spec (non-empty, ≤ 11 chars). */
    private void validateName(String name) {
        if (name == null || name.isEmpty() || name.length() > FENTRY_NAME_LEN) {
            throw new IllegalArgumentException("ERROR: filename too large");
        }
    }

    /** Find directory entry index or throw "does not exist". */
    private int findEntryOrThrow(String name) {
        int idx = findEntry(name);
        if (idx < 0) throw new IllegalStateException("ERROR: file " + name + " does not exist");
        return idx;
    }

    /** Linear search for filename in fentries (small MAX_FILES, so OK). */
    private int findEntry(String name) {
        for (int i = 0; i < MAX_FILES; i++) {
            String n = FS.nameOrEmpty(fentries[i]);
            if (!n.isEmpty() && n.equals(name)) return i;
        }
        return -1;
    }

    /** First empty directory slot index, or -1 if all used. */
    private int firstFreeFentry() {
        for (int i = 0; i < MAX_FILES; i++) {
            if (FS.nameOrEmpty(fentries[i]).isEmpty()) return i;
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
            FNode n = fnodes[fi];
            if (n == null) break;

            // Overwrite data for privacy
            if (n.blockIndex >= 0) {
                long off = blockOffset(n.blockIndex);
                byte[] zero = new byte[BLOCK_SIZE];
                disk.seek(off);
                disk.write(zero);
            }

            // Free node (mark unused)
            fnodes[fi].blockIndex = -1;
            int next = fnodes[fi].next;
            fnodes[fi].next = -1;
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
            FNode n = fnodes[i];
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

    /**
     * Load tables from disk, or initialize a fresh filesystem:
     * - fresh: create empty entries/nodes and zero out the data region once.
     * - existing: read both tables as-is.
     */
    private void loadOrInitTables(boolean fresh) throws IOException {
        if (fresh) {
            for (int i = 0; i < MAX_FILES; i++) fentries[i] = new FEntry("", (short) 0, (short) -1);
            for (int i = 0; i < MAX_BLOCKS; i++) { fnodes[i] = new FNode(-1); fnodes[i].next = -1; }
            writeFEntries();
            writeFNodes();

            // Zero data region (each block) once
            byte[] zero = new byte[BLOCK_SIZE];
            for (int b = 0; b < MAX_BLOCKS; b++) {
                disk.seek(blockOffset(dataStartBlock + b));
                disk.write(zero);
            }
            return;
        }
        readFEntries();
        readFNodes();
    }

    // -----------------------
    // Serialization helpers
    // -----------------------

    /** Persist all FEntries in order (fixed width). */
    private void writeFEntries() throws IOException {
        long off = fentryTableOffset;
        disk.seek(off);
        for (int i = 0; i < MAX_FILES; i++) {
            FEntry e = fentries[i];

            // name[11] (ASCII, padded with zeros)
            byte[] nameBytes = new byte[FENTRY_NAME_LEN];
            String n = FS.nameOrEmpty(e);
            if (!n.isEmpty()) {
                byte[] raw = n.getBytes(StandardCharsets.US_ASCII);
                System.arraycopy(raw, 0, nameBytes, 0, Math.min(raw.length, FENTRY_NAME_LEN));
            }
            disk.write(nameBytes);

            // size (u16) and first (i16)
            short size = (e == null) ? 0 : e.getFilesize();
            short fst  = (e == null) ? -1 : e.getFirstBlock();
            writeShort(size);
            writeShort(fst);
        }
    }

    /** Load all FEntries in order (fixed width). */
    private void readFEntries() throws IOException {
        long off = fentryTableOffset;
        disk.seek(off);
        for (int i = 0; i < MAX_FILES; i++) {
            byte[] nameBytes = new byte[FENTRY_NAME_LEN];
            disk.readFully(nameBytes);
            String name = new String(nameBytes, StandardCharsets.US_ASCII).trim();
            short size  = readShort();
            short fst   = readShort();
            fentries[i] = new FEntry(name, size, fst);
        }
    }

    /** Persist all FNodes in order. */
    private void writeFNodes() throws IOException {
        long off = fnodeTableOffset;
        disk.seek(off);
        for (int i = 0; i < MAX_BLOCKS; i++) {
            FNode n = fnodes[i];
            int bi = (n == null) ? -1 : n.blockIndex;
            int nx = (n == null) ? -1 : n.next;
            writeInt(bi);
            writeInt(nx);
        }
    }

    /** Load all FNodes in order. */
    private void readFNodes() throws IOException {
        long off = fnodeTableOffset;
        disk.seek(off);
        for (int i = 0; i < MAX_BLOCKS; i++) {
            int bi = readInt();
            int nx = readInt();
            FNode n = new FNode(bi);
            n.next = nx;
            fnodes[i] = n;
        }
    }

    // Primitive I/O (little-endian encoding for compactness)
    private void writeShort(short v) throws IOException {
        disk.write(v & 0xFF);
        disk.write((v >>> 8) & 0xFF);
    }
    private short readShort() throws IOException {
        int b0 = disk.read(); int b1 = disk.read();
        return (short) ((b0 & 0xFF) | ((b1 & 0xFF) << 8));
    }
    private void writeInt(int v) throws IOException {
        disk.write(v       & 0xFF);
        disk.write((v>>8)  & 0xFF);
        disk.write((v>>16) & 0xFF);
        disk.write((v>>24) & 0xFF);
    }
    private int readInt() throws IOException {
        int b0 = disk.read(), b1 = disk.read(), b2 = disk.read(), b3 = disk.read();
        return  (b0 & 0xFF)       |
                ((b1 & 0xFF) << 8) |
                ((b2 & 0xFF) << 16)|
                ((b3 & 0xFF) << 24);
    }

    /** Small utility for safe name extraction. */
    private static final class FS {
        static String nameOrEmpty(FEntry e) { return (e == null) ? "" : (e.getFilename() == null ? "" : e.getFilename().trim()); }
    }
}
