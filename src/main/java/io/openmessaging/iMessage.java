package io.openmessaging;

public class iMessage {
    Long offset;    // 在整个StoragePool中的偏移
    Short size;
    String fielname;
    Long currentBarrierOffset;
    public iMessage(Long offset, Short size) {
        this.offset = offset;
        this.size = size;
    }
    public iMessage(Long currentBarrierOffset, Long offset, Short size, String filename) {
        this.offset = offset;
        this.size = size;
        this.fielname = filename;
        this.currentBarrierOffset = currentBarrierOffset;
    }
}
