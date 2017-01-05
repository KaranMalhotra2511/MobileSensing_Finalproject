package com.example.android.pecvideostreaming.core;

/**
 * Created by user on 27-10-2016.
 */


        import com.example.android.pecvideostreaming.platform.Descriptor;
        import com.example.android.pecvideostreaming.utils.BloomFilter;

        import java.nio.ByteBuffer;
        import java.util.ArrayList;

public class Query extends Message {

    private int sender;
    private Descriptor descriptor;
    private BloomFilter<Descriptor> bloomFilter;

    public int getSender() { return sender; }
    public void setSender(int newValue) { reset(); sender = newValue; }

    public Descriptor getDescriptor() { return descriptor; }
    public void setDescriptor(Descriptor descriptor) { reset(); this.descriptor = descriptor; }

    public BloomFilter<Descriptor> getBloomFilter() { return bloomFilter; }
    public void setBloomFilter(BloomFilter<Descriptor> bloomFilter) { reset(); this.bloomFilter = bloomFilter; }

    public Query() { }

    public Query(byte[] wire) {
        this.wire = wire;
        decode();
    }

    public Query(Query q) {
        this.wire = q.encodeWire();
        decode();
    }

    public void addToBloomFilter(Descriptor descriptor) { reset(); bloomFilter.add(descriptor);}

    public void addToBloomFilter(ArrayList<Descriptor> descriptors) {
        for (Descriptor descriptor : descriptors)
            addToBloomFilter(descriptor);
    }

    @Override
    protected void encode() {
        byte[] descriptorWire = descriptor.encodeWire();
        byte[] bloomFilterWire = null;
        int bloomFilterSize = 0;
        if (bloomFilter != null)
            bloomFilterWire = bloomFilter.encodeWire();
        if (bloomFilterWire != null)
            bloomFilterSize = bloomFilterWire.length;

        ByteBuffer bb = ByteBuffer.allocate((1 + descriptorWire.length) * Byte.SIZE + (6 + receivers.size()) * Integer.SIZE + bloomFilterSize);
        bb.put(Type.Query.getValue());
        bb.putInt(nonce);
        bb.putInt(hopNonce);
        bb.putInt(receivers.size());
        for (int receiver : receivers) {
            bb.putInt(receiver);
        }
        bb.putInt(sender);
        bb.putInt(descriptorWire.length);
        bb.put(descriptorWire);
        bb.putInt(bloomFilterSize);
        if (bloomFilterWire != null)
            bb.put(bloomFilterWire);
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
        sender = bb.getInt();
        int descriptorLength = bb.getInt();
        byte[] descriptorWire = new byte[descriptorLength];
        bb.get(descriptorWire);
        descriptor = new Descriptor(descriptorWire);
        int bloomFilterSize = bb.getInt();
        if (bloomFilterSize > 0) {
            byte[] bloomFilterWire = new byte[bloomFilterSize];
            bb.get(bloomFilterWire);
            bloomFilter = BloomFilter.decodeWire(bloomFilterWire);
        } else {
            bloomFilter = null;
        }
    }
}

