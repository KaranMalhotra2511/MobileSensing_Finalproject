package com.example.android.pecvideostreaming.core;

/**
 * Created by user on 27-10-2016.
 */


        import com.example.android.pecvideostreaming.platform.Descriptor;

        import java.nio.ByteBuffer;

public class Response extends Message {

    private Descriptor descriptor;
    private byte[] payload;

    public byte[] getPayload() { return payload; }
    public void setPayload(byte[] payload) { reset(); this.payload = payload; }

    public Descriptor getDescriptor() { return descriptor; }
    public void setDescriptor(Descriptor descriptor) { reset(); this.descriptor = descriptor; }

    public Response() { }

    public Response(byte[] wire) {
        this.wire = wire;
        decode();
    }

    @Override
    protected void encode() {
        byte[] descriptorWire = descriptor.encodeWire();

        ByteBuffer bb = ByteBuffer.allocate((1 + descriptorWire.length + payload.length) * Byte.SIZE + (5 + receivers.size()) * Integer.SIZE);
        bb.put(Type.Response.getValue());
        bb.putInt(nonce);
        bb.putInt(hopNonce);
        bb.putInt(receivers.size());
        for (int receiver : receivers) {
            bb.putInt(receiver);
        }
        bb.putInt(descriptorWire.length);
        bb.put(descriptorWire);
        bb.putInt(payload.length);
        bb.put(payload);
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
        int descriptorLength = bb.getInt();
        byte[] descriptorWire = new byte[descriptorLength];
        bb.get(descriptorWire);
        descriptor = new Descriptor(descriptorWire);
        int payloadSize = bb.getInt();
        payload = new byte[payloadSize];
        bb.get(payload);
    }
}

