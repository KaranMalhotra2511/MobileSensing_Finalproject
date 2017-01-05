package com.example.android.pecvideostreaming.platform;

/**
 * Created by user on 27-10-2016.
 */


        import android.os.Parcel;
        import android.os.Parcelable;

        import java.nio.ByteBuffer;
        import java.util.HashMap;
        import java.util.SortedSet;
        import java.util.TreeSet;


/**
 * A Descriptor describes a data item or chunk.
 *
 * A Descriptor contains a DataType, a ChunkAmount, a ChunkId and a set of Attributes.
 *
 * DataType is application defined integer. Namespace is omitted for simplification. Application
 * builders should define their own unique data types. NOTE: Application builders should never
 * define a DataType with negative value.
 *
 * ChunkAmount indicates how many chunks exist for a data item. Set ChunkAmount to -1 if not
 * applicable. E.g., requesting for metadata that the amount of chunks is unknown.
 *
 * ChunkId indicates which chunk of the data item is described. It should be set to -1 if describing
 * the whole data item.
 *
 * Attributes are represented by String type key-value pairs.
 */
public class Descriptor implements Parcelable {

    // constants

    public static final int DATA_TYPE_METADATA = -1;
    public static final int DATA_TYPE_CDI = -2;
    public static final int DATA_TYPE_CHUNK = -3;

    // attributes

    private int dataType;   // A descriptor should contain namespace, data type and attributes.
    // We use a simple integer to distinguish different data items for
    // experiment simplicities.
    private int chunkAmount;    // Set to -1 if chunk amount is not determined
    private int chunkId;    // Set to -1 if describing the entire data item instead of any single chunk
    private HashMap<String, String> attributes = new HashMap<>();


    // getters and setters

    public int getDataType() { return dataType; }
    public void setDataType(int dataType) { this.dataType = dataType; }

    public int getChunkAmount() { return chunkAmount; }
    public void setChunkAmount(int chunkAmount) { this.chunkAmount = chunkAmount; }

    public int getChunkId() { return chunkId; }
    public void setChunkId(int chunkId) { this.chunkId = chunkId; }

    public HashMap<String, String> getAttributes() { return attributes; }
    public void setAttributes( HashMap<String, String> attributes) { this.attributes = attributes; }

    // constructors

    public Descriptor(){
        chunkAmount = -1;
        chunkId = -1;
    }

    public Descriptor(int dataType, int chunkAmount, int chunkId) {
        this.dataType = dataType;
        this.chunkAmount = chunkAmount;
        this.chunkId = chunkId;
    }

    public Descriptor(int dataType, int chunkAmount, int chunkId, HashMap<String, String> attributes) {
        this.dataType = dataType;
        this.chunkAmount = chunkAmount;
        this.chunkId = chunkId;
        this.attributes = attributes;
    }

    public Descriptor(Descriptor descriptor) {
        this.dataType = descriptor.dataType;
        this.chunkAmount = descriptor.chunkAmount;
        this.chunkId = descriptor.chunkId;
        this.attributes = (HashMap<String, String>) descriptor.attributes.clone();
    }

    /**
     * Constructs a Descriptor by decoding a bytes array received from the network. This constructor
     * should always be consistent with the encodeWire() method.
     */
    public Descriptor(byte[] wire) {
        ByteBuffer bb = ByteBuffer.wrap(wire);
        dataType = bb.getInt();
        chunkAmount = bb.getInt();
        chunkId = bb.getInt();
        attributes.clear();
        int attributeNum = bb.getInt();
        while (attributeNum-- > 0) {
            int keySize = bb.getInt();
            byte[] key = new byte[keySize];
            bb.get(key);
            int valueSize = bb.getInt();
            byte[] value = new byte[valueSize];
            attributes.put(new String(key), new String (value));
        }
    }

    /**
     * Encode a Descriptor into a bytes array. This method should always be consistent with the
     * constructor Descriptor(byte[] wire).
     */
    public byte[] encodeWire() {
        ByteBuffer bb = ByteBuffer.allocate(3 * Integer.SIZE + (int) getAttributesSize());
        bb.putInt(dataType);
        bb.putInt(chunkAmount);
        bb.putInt(chunkId);
        bb.putInt(attributes.size());
        for (String key : attributes.keySet()) {
            String value = attributes.get(key);
            bb.putInt(key.length());
            bb.put(key.getBytes());
            bb.putInt(value.length());
            bb.put(value.getBytes());
        }
        return bb.array();
    }

    private long getAttributesSize() {
        long size = Integer.SIZE;
        for (String key : attributes.keySet()) {
            size += 2 * Integer.SIZE;
            size += key.length() * Character.SIZE;
            size += attributes.get(key).length() * Character.SIZE;
        }
        return size;
    }

    @Override
    public String toString() {
        String s = "[DataType:" + dataType + ",ChunkAmount:" + chunkAmount + ",ChunkId:" + chunkId;
        SortedSet<String> keys = new TreeSet<>(attributes.keySet());
        for (String key : keys) {
            s += "," + key + ":" + attributes.get(key);
        }
        s += "]";
        return s;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof  Descriptor)) {
            return false;
        }

        Descriptor descriptor = (Descriptor) o;

        if (this.dataType != descriptor.dataType
                || this.chunkAmount != descriptor.chunkAmount
                || this.chunkId != descriptor.chunkId
                || this.attributes.size() != descriptor.attributes.size())
            return false;

        for (String key : this.attributes.keySet()) {
            if (!descriptor.attributes.containsKey(key)
                    || !this.attributes.get(key).equals(descriptor.attributes.get(key))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int describeContents() {
        return 0;
    }

    @Override
    public void writeToParcel(Parcel dest, int flags) {
        dest.writeInt(dataType);
        dest.writeInt(chunkAmount);
        dest.writeInt(chunkId);
        dest.writeSerializable(attributes);
    }

    public static final Parcelable.Creator<Descriptor> CREATOR = new Parcelable.Creator<Descriptor>() {
        public Descriptor createFromParcel(Parcel in) {
            return new Descriptor(in);
        }

        public Descriptor[] newArray(int size) {
            return new Descriptor[size];
        }
    };

    private Descriptor(Parcel in) {
        dataType = in.readInt();
        chunkAmount = in.readInt();
        chunkId = in.readInt();
        attributes = (HashMap<String, String>)in.readSerializable();
    }
}

