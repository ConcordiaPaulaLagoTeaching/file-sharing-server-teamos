package ca.concordia.filesystem.datastructures;

public class FNode {

    public int blockIndex;
    public int next;

    public FNode(int blockIndex) {
        this.blockIndex = blockIndex;
        this.next = -1;
    }
}
