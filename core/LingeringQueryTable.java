package com.example.android.pecvideostreaming.core;

/**
 * Created by user on 27-10-2016.
 */
        import com.example.android.pecvideostreaming.platform.Descriptor;

        import java.util.ArrayList;
        import java.util.HashMap;
        import java.util.List;
        import java.util.Map;

public class LingeringQueryTable {

    private Map<Integer, Query> table; // nonce -> query

    public LingeringQueryTable() {
        table = new HashMap<>();
    }

    public boolean exist(int nonce) {
        return table.containsKey(nonce);
    }

    public void addQuery(Query query) {
        table.put(query.getNonce(), new Query(query));
    }

    public List<Integer> getReceivers(Response response) {
        List<Integer> ret = new ArrayList<>();
        for (Query q : table.values()) {
            if (response.getDescriptor().equals(q.getDescriptor())) {
                ret.add(q.getSender());
            }
        }
        return ret;
    }

    public ArrayList<Descriptor> getIrredundantMetadata(ArrayList<Descriptor> metadata) {
        ArrayList<Query> metadataQueries = new ArrayList<>();
        for (Query q : table.values()) {
            if (q.getDescriptor().getDataType() == Descriptor.DATA_TYPE_METADATA) {
                metadataQueries.add(q);
            }
        }
        ArrayList<Descriptor> irredundantMetadata = new ArrayList<>();
        for (Descriptor d : metadata) {
            for (Query q : metadataQueries) {
                if (!q.getBloomFilter().contains(d)) {
                    irredundantMetadata.add(d);
                    break;
                }
            }
        }
        return irredundantMetadata;
    }

    public ArrayList<Integer> getReceiversForMetadata(ArrayList<Descriptor> metadata) {
        ArrayList<Integer> ret = new ArrayList<>();
        for (Query q : table.values()) {
            if (q.getDescriptor().getDataType() == Descriptor.DATA_TYPE_METADATA) {
                for (Descriptor d : metadata) {
                    if (!q.getBloomFilter().contains(d)) {
                        ret.add(q.getSender());
                        break;
                    }
                }
            }
        }
        return ret;
    }
}

