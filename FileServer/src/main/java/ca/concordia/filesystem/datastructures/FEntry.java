package ca.concordia.filesystem.datastructures;

/* FEntry class is a data structure that represents an entry in the filesystem (metadata).
 * It contains the filename, file size and the first block index in the chain.
 * If firstBlock is -1, it means there is no data content in the file
 */
public class FEntry {

    private String filename;
    private short filesize;
    private short firstBlock; // Pointers to data blocks

    // Constructor for FEntry class
    public FEntry(String filename, short filesize, short firstblock) throws IllegalArgumentException {
        // Check filename is max 11 bytes long
        if (filename.length() > 11) {
            throw new IllegalArgumentException("Filename cannot be longer than 11 characters.");
        }
        this.filename = filename;
        this.filesize = filesize;
        this.firstBlock = firstblock;
    }

    // Getters and Setters

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        if (filename.length() > 11) {
            throw new IllegalArgumentException("Filename cannot be longer than 11 characters.");
        }
        this.filename = filename;
    }

    public short getFilesize() {
        return filesize;
    }

    public void setFilesize(short filesize) {
        if (filesize < 0) {
            throw new IllegalArgumentException("Filesize cannot be negative.");
        }
        this.filesize = filesize;
    }

    public short getFirstBlock() {
        return firstBlock;
    }
}
