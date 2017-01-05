package com.example.android.pecvideostreaming.core;

/**
 * Created by user on 27-10-2016.
 */

        import com.example.android.pecvideostreaming.platform.Descriptor;

        import java.util.ArrayList;
        import java.util.HashMap;
        import java.util.HashSet;
        import java.util.List;
        import java.util.Map;
        import java.util.Set;

public class DataStore {

    private class DataStoreDataItem {
        DataStoreDataItem(Descriptor descriptor) {
            this.descriptor = descriptor;
        }

        Descriptor descriptor;
        Map<Integer, DataStoreChunkItem> chunkList = new HashMap<>(); // Map: ChunkId -> ChunkItem
    }

    private class DataStoreChunkItem {
        int hopCount;
        Set<Integer> neighborIds = new HashSet<>();
        byte[] data = null;
    }

    private List<DataStoreDataItem> items;

    public DataStore() {
        items = new ArrayList<>();
    }

    public void addData(Descriptor descriptor, byte[] data) {
        Descriptor des = new Descriptor(descriptor);
        des.setChunkId(-1);

        DataStoreDataItem dataItem = findDataItem(des);
        if (dataItem == null) {
            dataItem = new DataStoreDataItem(des);
            items.add(dataItem);
        }

        DataStoreChunkItem chunkItem = dataItem.chunkList.get(descriptor.getChunkId());
        if (chunkItem == null) {
            chunkItem = new DataStoreChunkItem();
            dataItem.chunkList.put(descriptor.getChunkId(), chunkItem);
        }

        chunkItem.data = data;
        chunkItem.hopCount = 0;
        chunkItem.neighborIds.clear();
    }

    public byte[] getData(Descriptor descriptor) {
        Descriptor des = new Descriptor(descriptor);
        des.setChunkId(-1);

        DataStoreDataItem dataItem = findDataItem(des);
        if (dataItem == null)
            return null;

        DataStoreChunkItem chunkItem = dataItem.chunkList.get(descriptor.getChunkId());
        if (chunkItem == null)
            return null;

        if (chunkItem.data == null)
            return null;

        return chunkItem.data.clone();
    }

    public void addMetadata(Descriptor descriptor) {
        Descriptor des = new Descriptor(descriptor);
        des.setChunkId(-1);

        DataStoreDataItem dataItem = findDataItem(des);
        if (dataItem == null) {
            dataItem = new DataStoreDataItem(des);
            items.add(dataItem);
        }

        if (descriptor.getChunkId() == -1) {
            for (int i = 0; i < descriptor.getChunkAmount(); ++i) {
                DataStoreChunkItem chunkItem = dataItem.chunkList.get(i);
                if (chunkItem == null) {
                    chunkItem = new DataStoreChunkItem();
                    dataItem.chunkList.put(descriptor.getChunkId(), chunkItem);
                }
                chunkItem.hopCount = 0;
                chunkItem.neighborIds.clear();
            }
        } else {
            DataStoreChunkItem chunkItem = dataItem.chunkList.get(descriptor.getChunkId());
            if (chunkItem == null) {
                chunkItem = new DataStoreChunkItem();
                dataItem.chunkList.put(descriptor.getChunkId(), chunkItem);
            }
            chunkItem.hopCount = 0;
            chunkItem.neighborIds.clear();
        }
    }

    public ArrayList<Descriptor> getMetadata() {
        ArrayList<Descriptor> metadata = new ArrayList<>();
        for (DataStoreDataItem dataItem : items) {
            Descriptor descriptor = new Descriptor(dataItem.descriptor);
            descriptor.setChunkId(-1);
            metadata.add(descriptor);
        }
        return metadata;
    }

    private final DataStoreDataItem findDataItem(Descriptor descriptor) {
        DataStoreDataItem ret = null;
        for (DataStoreDataItem item : items) {
            if (item.descriptor.equals(descriptor)) {
                ret = item;
                break;
            }
        }
        return ret;
    }


}
