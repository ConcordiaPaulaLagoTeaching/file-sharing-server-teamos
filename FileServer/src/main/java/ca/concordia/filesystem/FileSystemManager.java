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

public class FileSystemManager {

    private final int BLOCK_SIZE = 128;
    private final int MAX_FILES  = 5;
    private final int MAX_BLOCKS;

    private static final int FENTRY_NAME_LEN = 11;
    private static final int FENTRY_BYTES     = 11 + 2 + 2;
    private static final int FNODE_BYTES      = 4 + 4;

    private final RandomAccessFile disk;
    private final long fentryTableOffset;
    private final long fnodeTableOffset;
    private final int  metaBytes;
    private final int  metaBlocks;
    private final int  dataStartBlock;
    private final long imageBytes;

    private final FEntry[] fentries;
    private final FNode[]  fnodes;

    private final BitSet usedDataBlocks;
    private final BitSet usedFnodes;

    private final ReentrantReadWriteLock rw = new ReentrantReadWriteLock(true);

    public FileSystemManager(String filename, int totalSizeBytes) {
        try {
            if (totalSizeBytes <= 0 || (totalSizeBytes % BLOCK_SIZE) != 0) {
                throw new IllegalArgumentException("totalSize must be a positive multiple of BLOCK_SIZE");
            }
            this.MAX_BLOCKS = totalSizeBytes / BLOCK_SIZE;

            int feBytes = MAX_FILES * FENTRY_BYTES;
            int fnBytes = MAX_BLOCKS * FNODE_BYTES;
            this.metaBytes = feBytes + fnBytes;
            this.metaBlocks = (int) Math.ceil(metaBytes / (double) BLOCK_SIZE);
            this.fentryTableOffset = 0L;
            this.fnodeTableOffset  = fentryTableOffset + feBytes;
            this.dataStartBlock    = metaBlocks;

            this.imageBytes = (long) (metaBlocks + MAX_BLOCKS) * BLOCK_SIZE;

            this.disk = new RandomAccessFile(filename, "rw");
            long oldLen = disk.length();
            if (disk.length() < imageBytes) {
                disk.setLength(imageBytes);
            }

            this.fentries = new FEntry[MAX_FILES];
            this.fnodes   = new FNode[MAX_BLOCKS];
            loadOrInitTables(oldLen == 0);

            this.usedDataBlocks = new BitSet(MAX_BLOCKS);
            this.usedFnodes     = new BitSet(MAX_BLOCKS);
            rebuildBitmaps();

        } catch (IOException ioe) {
            throw new RuntimeException("Failed to open filesystem image", ioe);
        }
    }

    public void createFile(String fileName) throws Exception {
        validateName(fileName);
        rw.writeLock().lock();
        try {
            if (findEntry(fileName) >= 0) {
                throw new IllegalStateException("ERROR: file " + fileName + " already exists");
            }
            int slot = firstFreeFentry();
            if (slot < 0) {
                throw new IllegalStateException("ERROR: file too large");
            }
            FEntry e = new FEntry(fileName, (short) 0, (short) -1);
            fentries[slot] = e;
            writeFEntries();
        } finally {
            rw.writeLock().unlock();
        }
    }

    public void deleteFile(String fileName) throws Exception {
        rw.writeLock().lock();
        try {
            int ei = findEntryOrThrow(fileName);
            FEntry e = fentries[ei];
            freeChainZero(e.getFirstBlock());
            fentries[ei] = new FEntry("", (short) 0, (short) -1);
            writeFEntries();
            writeFNodes();
        } finally {
            rw.writeLock().unlock();
        }
    }

    public void writeFile(String fileName, byte[] contents) throws Exception {
        rw.writeLock().lock();
        try {
            int ei = findEntryOrThrow(fileName);
            FEntry old = fentries[ei];

            int size = (contents == null) ? 0 : contents.length;
            if (size > 0xFFFF) {
                throw new IllegalStateException("ERROR: file too large");
            }

            int blocksNeeded = (size + BLOCK_SIZE - 1) / BLOCK_SIZE;

            List<Integer> freeData = collectFreeDataBlocks(blocksNeeded);
            List<Integer> freeFn   = collectFreeFnodes(blocksNeeded);
            if (blocksNeeded > 0 && (freeData.size() < blocksNeeded || freeFn.size() < blocksNeeded)) {
                throw new IllegalStateException("ERROR: file too large");
            }

            int pos = 0;
            for (int k = 0; k < blocksNeeded; k++) {
                int dataBlockIdx = dataStartBlock + freeData.get(k);
                long off = blockOffset(dataBlockIdx);
                byte[] buf = new byte[BLOCK_SIZE];
                int len = Math.min(BLOCK_SIZE, size - pos);
                if (len > 0) System.arraycopy(contents, pos, buf, 0, len);
                disk.seek(off);
                disk.write(buf);
                pos += len;
            }

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
            writeFNodes();

            short newSizeU16 = (short) (size & 0xFFFF);
            short newHeadS16 = (short) ((head == null) ? -1 : head.intValue());
            fentries[ei].setFilesize(newSizeU16);
            fentries[ei] = new FEntry(fentries[ei].getFilename(), newSizeU16, newHeadS16);
            writeFEntries();

            if (old.getFirstBlock() >= 0) {
                freeChainZero(old.getFirstBlock());
                writeFNodes();
            }

            rebuildBitmaps();

        } finally {
            rw.writeLock().unlock();
        }
    }

    public byte[] readFile(String fileName) throws Exception {
        rw.readLock().lock();
        try {
            int ei = findEntryOrThrow(fileName);
            FEntry e = fentries[ei];
            int remaining = Short.toUnsignedInt(e.getFilesize());
            if (remaining == 0) return new byte[0];

            List<byte[]> chunks = new ArrayList<>();
            int total = 0;
            int fi = e.getFirstBlock();
            while (fi != -1 && remaining > 0) {
                FNode n = fnodes[fi];
                if (n == null || n.blockIndex < 0) {
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
            byte[] out = new byte[Short.toUnsignedInt(e.getFilesize())];
            int p = 0;
            for (byte[] c : chunks) { System.arraycopy(c, 0, out, p, c.length); p += c.length; }
            return out;
        } finally {
            rw.readLock().unlock();
        }
    }

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

    private void validateName(String name) {
        if (name == null || name.isEmpty() || name.length() > FENTRY_NAME_LEN) {
            throw new IllegalArgumentException("ERROR: filename too large");
        }
    }

    private int findEntryOrThrow(String name) {
        int idx = findEntry(name);
        if (idx < 0) throw new IllegalStateException("ERROR: file " + name + " does not exist");
        return idx;
    }

    private int findEntry(String name) {
        for (int i = 0; i < MAX_FILES; i++) {
            String n = FS.nameOrEmpty(fentries[i]);
            if (!n.isEmpty() && n.equals(name)) return i;
        }
        return -1;
    }

    private int firstFreeFentry() {
        for (int i = 0; i < MAX_FILES; i++) {
            if (FS.nameOrEmpty(fentries[i]).isEmpty()) return i;
        }
        return -1;
    }

    private void freeChainZero(int head) throws IOException {
        int fi = head;
        while (fi != -1) {
            FNode n = fnodes[fi];
            if (n == null) break;
            if (n.blockIndex >= 0) {
                long off = blockOffset(n.blockIndex);
                byte[] zero = new byte[BLOCK_SIZE];
                disk.seek(off);
                disk.write(zero);
            }
            fnodes[fi].blockIndex = -1;
            int next = fnodes[fi].next;
            fnodes[fi].next = -1;
            fi = next;
        }
        rebuildBitmaps();
    }

    private List<Integer> collectFreeDataBlocks(int needed) {
        List<Integer> out = new ArrayList<>(needed);
        if (needed == 0) return out;
        for (int i = 0; i < MAX_BLOCKS && out.size() < needed; i++) {
            if (!usedDataBlocks.get(i)) out.add(i);
        }
        return out;
    }

    private List<Integer> collectFreeFnodes(int needed) {
        List<Integer> out = new ArrayList<>(needed);
        if (needed == 0) return out;
        for (int i = 0; i < MAX_BLOCKS && out.size() < needed; i++) {
            if (!usedFnodes.get(i)) out.add(i);
        }
        return out;
    }

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

    private long blockOffset(int absoluteBlockIndex) {
        return (long) absoluteBlockIndex * BLOCK_SIZE;
    }

    private void loadOrInitTables(boolean fresh) throws IOException {
        if (fresh) {
            for (int i = 0; i < MAX_FILES; i++) fentries[i] = new FEntry("", (short) 0, (short) -1);
            for (int i = 0; i < MAX_BLOCKS; i++) { fnodes[i] = new FNode(-1); fnodes[i].next = -1; }
            writeFEntries();
            writeFNodes();
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

    private void writeFEntries() throws IOException {
        long off = fentryTableOffset;
        disk.seek(off);
        for (int i = 0; i < MAX_FILES; i++) {
            FEntry e = fentries[i];
            byte[] nameBytes = new byte[FENTRY_NAME_LEN];
            String n = FS.nameOrEmpty(e);
            if (!n.isEmpty()) {
                byte[] raw = n.getBytes(StandardCharsets.US_ASCII);
                System.arraycopy(raw, 0, nameBytes, 0, Math.min(raw.length, FENTRY_NAME_LEN));
            }
            disk.write(nameBytes);
            short size = (e == null) ? 0 : e.getFilesize();
            short fst  = (e == null) ? -1 : e.getFirstBlock();
            writeShort(size);
            writeShort(fst);
        }
    }

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

    private static final class FS {
        static String nameOrEmpty(FEntry e) { return (e == null) ? "" : (e.getFilename() == null ? "" : e.getFilename().trim()); }
    }
}
