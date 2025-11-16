package ca.concordia.filesystem.datastructures;

/* FNode class is a data structure that represents a node in the filesystem (metadata).
 * It contains the block index of the data block and the next fnode index in the chain.
 * If next is -1, it means there are no more blocks in the chain.
 */ 
public class FNode {

    public int blockIndex;
    public int next;

    public FNode(int blockIndex) {
        this.blockIndex = blockIndex;
        this.next = -1;
    }

    public FNode(int blockIndex, int next) {
        this.blockIndex = blockIndex;
        this.next = next;
    }
}
