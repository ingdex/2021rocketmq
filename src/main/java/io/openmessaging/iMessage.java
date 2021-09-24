package io.openmessaging;

public class iMessage {
    Long offset;    // 在整个StoragePool中的偏移
    int size;
    String fielname;
    Long currentBarrierOffset;
    public iMessage(Long offset, Short size) {
        this.offset = offset;
        this.size = size;
    }
    public iMessage(Long currentBarrierOffset, Long offset, int size, String filename) {
        this.offset = offset;
        this.size = size;
        this.fielname = filename;
        this.currentBarrierOffset = currentBarrierOffset;
    }
}
