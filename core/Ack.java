package com.example.android.pecvideostreaming.core;

/**
 * Created by user on 27-10-2016.
 */

    import java.nio.ByteBuffer;

public class Ack extends Message {

    private int from;
    public int getFrom() { return from; }
    public void setFrom(int newValue) { reset(); from = newValue; }

    public Ack() { }

    public Ack(int nonce, int hopNonce, int from) {
        this.nonce = nonce;
        this.hopNonce = hopNonce;
        this.from = from;
    }

    public Ack(byte[] wire) {
        this.wire = wire;
        decode();
    }

    @Override
    protected void encode() {
        ByteBuffer bb = ByteBuffer.allocate(1 * Byte.SIZE + 3 * Integer.SIZE);
        bb.put(Type.Ack.getValue());
        bb.putInt(nonce);
        bb.putInt(hopNonce);
        bb.putInt(receivers.size());
        for (int receiver : receivers) {
            bb.putInt(receiver);
        }
        bb.putInt(from);
        wire = bb.array();
    }

    @Override
    protected void decode() {
        ByteBuffer bb = ByteBuffer.wrap(wire);
        bb.get(); // type
        nonce = bb.getInt();
        hopNonce = bb.getInt();
        receivers.clear();
        int receiverNum = bb.getInt();
        while (receiverNum-- > 0) {
            receivers.add(bb.getInt());
        }
        from = bb.getInt();
    }
}

