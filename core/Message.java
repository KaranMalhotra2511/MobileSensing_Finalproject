package com.example.android.pecvideostreaming.core;

/**
 * Created by user on 27-10-2016.
 */

        import java.util.ArrayList;
        import java.util.Date;
        import java.util.List;
        import java.util.Random;

public abstract class Message {

    public enum Type {
        Unknown((byte)0), Query((byte)1), Response((byte)2), Ack((byte)3);

        public static Type fromValue(byte v) {
            switch (v) {
                case (byte)1: return Query;
                case (byte)2: return Response;
                case (byte)3: return Ack;
            }
            return Unknown;
        }

        public byte getValue() { return value; }

        Type(byte v) {
            value = v;
        }
        private byte value;
    }

    public static Type peekMessageType(byte[] wire) {
        return Type.fromValue(wire[0]);
    }

    public byte[] encodeWire() {
        if (wire == null)
            encode();
        return wire.clone();
    }

    public String getType() {
        switch(Type.fromValue(wire[0])) {
            case Query: return "Q";
            case Response: return "R";
            case Ack: return "A";
        }
        return "U";
    }

    public int getNonce() { return nonce; }
    public void setNonce(int newValue) { reset(); nonce = newValue; }

    public int getHopNonce() { return hopNonce; }
    public void setHopNonce(int newValue) { reset(); hopNonce = newValue; }

    public List<Integer> getReceivers() { return receivers; }
    public void setReceivers(List<Integer> newValue) { reset(); receivers = newValue; }

    protected abstract void encode();
    protected abstract void decode();

    protected void reset() {
        wire = null;
    }

    protected byte[] wire;
    protected int nonce = new Random(new Date().getTime()).nextInt();
    protected int hopNonce;
    protected List<Integer> receivers = new ArrayList<>();
}

