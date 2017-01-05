package com.example.android.pecvideostreaming.core;

/**
 * Created by user on 27-10-2016.
 */


        import com.example.android.pecvideostreaming.platform.Descriptor;
        import com.example.android.pecvideostreaming.platform.PecLog;
        import com.example.android.pecvideostreaming.platform.PecService;
        import com.example.android.pecvideostreaming.utils.BloomFilter;

        import java.nio.ByteBuffer;
        import java.util.ArrayList;
        import java.util.Date;
        import java.util.HashMap;
        import java.util.HashSet;
        import java.util.LinkedList;
        import java.util.List;
        import java.util.Queue;
        import java.util.Random;
        import java.util.Set;
        import java.util.Timer;
        import java.util.TimerTask;

public class Forwarder {

    private int deviceId;
    PecService context;

    private DataStore dataStore;
    private LingeringQueryTable lingeringQueryTable;
    private Set<Integer> recentResponses;
    private NetworkAdapter networkAdapter;

    private HashMap<Descriptor, Integer> localQueries; // descriptors of queries comes from local applications
    private HashMap<Descriptor, Integer> localData; // descriptors of data that local applications have
    private Set<Integer> localMetadataQueries; // ids of applications requesting for metadata

    private static Random random = new Random(new Date().getTime());
    private static Timer timer = new Timer();

    // redundancy detection parameters
    private static final int BF_MAX_SIZE = 10000 * 8;
    private static final int BF_MIN_SIZE = 1000 * 8;
    private static final double BF_FPP = 0.01;

    // PDS parameters and state information
    private static final double PDS_ROUND_FINISH_THRESHOLD = 0;
    private static final double PDS_DISCOVERY_FINISH_THRESHOLD = 0;
    private static final long PDS_SLOT_SIZE = 200;
    private static final int PDS_WINDOW_SIZE = 10;
    private int pdsMetadataCountDiscovery;
    private int pdsMetadataCountRound;
    private int pdsResponseCountRound;
    private int pdsResponseCountSlot;
    private int pdsResponseCountWindow;
    private Queue<Integer> pdsWindow;

    public Forwarder(int deviceId, PecService context) {
        this.deviceId = deviceId;
        this.context = context;

        localQueries = new HashMap<>();
        localData = new HashMap<>();
        localMetadataQueries = new HashSet<>();

        pdsWindow = new LinkedList<>();

        dataStore = new DataStore();
        lingeringQueryTable = new LingeringQueryTable();
        recentResponses = new HashSet<>();
        networkAdapter = new NetworkAdapter(this, deviceId);
    }

    public void startListening() {
        networkAdapter.startListening();
    }


    public void onReceiveQuery(Query query) {
        PecLog.d("receive query");

        // loop detection
        if (lingeringQueryTable.exist(query.getNonce()))
            return;

        Descriptor descriptor = query.getDescriptor();

        // request for metadata
        if (descriptor.getDataType() == Descriptor.DATA_TYPE_METADATA) {
            // redundancy detection
            ArrayList<Descriptor> metadata = dataStore.getMetadata();
            ArrayList<Descriptor> irredundantMetadata = new ArrayList<>();
            for (Descriptor des : metadata) {
                if (!query.getBloomFilter().contains(des)) {
                    irredundantMetadata.add(des);
                }
            }
            // rewrite and cache query
            query.addToBloomFilter(irredundantMetadata);
            lingeringQueryTable.addQuery(new Query(query));

            // send query
            query.setSender(deviceId);
            query.setHopNonce(random.nextInt());
            networkAdapter.sendMessage(query, 0);

            if (irredundantMetadata.isEmpty())
                return;

            // generate response
            int payloadSize = Integer.SIZE;
            for (Descriptor des : irredundantMetadata) {
                payloadSize += Integer.SIZE + des.encodeWire().length;
            }
            ByteBuffer bb = ByteBuffer.allocate(payloadSize);
            bb.putInt(irredundantMetadata.size());
            for (Descriptor des : irredundantMetadata) {
                byte[] desWire = des.encodeWire();
                bb.putInt(desWire.length);
                bb.put(desWire);
            }
            Response response = new Response();
            response.setDescriptor(descriptor);
            response.setPayload(bb.array());

            // send response
            ArrayList<Integer> receivers = new ArrayList<>();
            receivers.add(query.getSender());
            response.setReceivers(receivers);
            response.setHopNonce(random.nextInt());
            networkAdapter.sendMessage(response, NetworkAdapter.DEFAULT_MAX_RETRANSMIT);
        }

        // request for a normal data chunk
        else {
            lingeringQueryTable.addQuery(new Query(query));

            byte[] data = dataStore.getData(descriptor);

            // If find data in DataStore, reply by sending response.
            if (data != null) {
                Response response = new Response();
                response.setDescriptor(descriptor);
                response.setPayload(data);

                ArrayList<Integer> receivers = new ArrayList<>();
                receivers.add(query.getSender());
                response.setReceivers(receivers);

                for (int i : response.getReceivers()) {
                    PecLog.d("send to receiver " + i);
                }

                response.setHopNonce(random.nextInt());
                networkAdapter.sendMessage(response, NetworkAdapter.DEFAULT_MAX_RETRANSMIT);

                return;
            }

            // If find data in local applications, request data from the application.
            for (Descriptor d : localData.keySet()) {
                if (d.equals(descriptor)) {
                    context.serviceRequestData(descriptor, localData.get(d));
                    return;
                }
            }

            // If data are not find, keep forwarding the query.
            query.setSender(deviceId);
            query.setHopNonce(random.nextInt());
            networkAdapter.sendMessage(query, 0);
        }

    }

    public void onReceiveResponse(Response response) {
        PecLog.d("receive response");
        for (int i : response.getReceivers()) {
            PecLog.d("receiver " + i);
        }

        // loop detection
        if (recentResponses.contains(response.getNonce()))
            return;
        recentResponses.add(response.getNonce());

        Descriptor descriptor = response.getDescriptor();

        // receive metadata
        if (descriptor.getDataType() == Descriptor.DATA_TYPE_METADATA) {

            // decode metadata

            ByteBuffer bb = ByteBuffer.wrap(response.getPayload());
            int metadataNum = bb.getInt();
            ArrayList<Descriptor> metadata = new ArrayList<>();
            while (metadataNum-- > 0) {
                int desSize = bb.getInt();
                byte[] desWire = new byte[desSize];
                bb.get(desWire);
                Descriptor des = new Descriptor(desWire);
                metadata.add(des);
            }

            // find irredundant metadata

            ArrayList<Descriptor> irredundantMetadata = new ArrayList<>();
            ArrayList<Descriptor> localMetadata = dataStore.getMetadata();
            for (Descriptor des : metadata) {
                if (!localMetadata.contains(des)) {
                    irredundantMetadata.add(des);
                }
            }

            // update statistics

            pdsMetadataCountDiscovery += irredundantMetadata.size();
            pdsMetadataCountRound += irredundantMetadata.size();
            ++pdsResponseCountSlot;
            ++pdsResponseCountRound;

            // cache and provide to app

            if (!irredundantMetadata.isEmpty()) {
                for (Descriptor des : irredundantMetadata) {
                    dataStore.addMetadata(des);
                }
                for (int appId : localMetadataQueries) {
                    context.serviceProvideMetadata(irredundantMetadata, appId);
                }
            }

            // find union set of irredundant metadata for all lingering queries
            irredundantMetadata = lingeringQueryTable.getIrredundantMetadata(irredundantMetadata);
            ArrayList<Integer> receivers = lingeringQueryTable.getReceiversForMetadata(irredundantMetadata);

            if (irredundantMetadata.isEmpty())
                return;

            // rewrite and send response
            int payloadSize = Integer.SIZE;
            for (Descriptor des : irredundantMetadata) {
                payloadSize += Integer.SIZE + des.encodeWire().length;
            }
            bb = ByteBuffer.allocate(payloadSize);
            bb.putInt(irredundantMetadata.size());
            for (Descriptor des : irredundantMetadata) {
                byte[] desWire = des.encodeWire();
                bb.putInt(desWire.length);
                bb.put(desWire);
            }
            response.setPayload(bb.array());

            response.setReceivers(receivers);
            response.setHopNonce(random.nextInt());
            networkAdapter.sendMessage(response, NetworkAdapter.DEFAULT_MAX_RETRANSMIT);
        }

        // receive one normal data chunk
        else {
            // add data to DataStore
            dataStore.addData(descriptor, response.getPayload());

            // if the current node is specified as receiver, it should keep forwarding the response
            if (response.getReceivers().contains(deviceId)) {
                List<Integer> receivers = lingeringQueryTable.getReceivers(response);
                if (receivers.size() > 0) {
                    response.setReceivers(receivers);
                    response.setHopNonce(random.nextInt());
                    networkAdapter.sendMessage(response, NetworkAdapter.DEFAULT_MAX_RETRANSMIT);
                }
            }

            // if the data is requested by any local application, provide the data to the application
            ArrayList<Descriptor> removeList = new ArrayList<>();
            for (Descriptor d : localQueries.keySet()) {
                if (d.equals(descriptor)) {
                    context.serviceProvideData(descriptor, response.getPayload(), localQueries.get(d));
                    removeList.add(d);
                }
            }
            for (Descriptor d : removeList) {
                localQueries.remove(d);
            }
        }
    }

    // Application provides data to PecService.
    public void appProvideData(Descriptor descriptor, byte[] data, int appId) {

        // Add data to PecService

        localData.put(descriptor, appId);
        dataStore.addData(descriptor, data);

        // Check if there are any lingering queries requesting for the provided data. If positive,
        // send responses to provide data.

        Response response = new Response();
        response.setDescriptor(descriptor);
        response.setPayload(data);

        List<Integer> receivers = lingeringQueryTable.getReceivers(response);
        if (receivers.size() > 0) {
            response.setReceivers(receivers);
            response.setHopNonce(random.nextInt());
            networkAdapter.sendMessage(response, NetworkAdapter.DEFAULT_MAX_RETRANSMIT);
        }
    }

    // Application provides metadata to PecService.
    public void appProvideMetadata(ArrayList<Descriptor> metadata, int appId) {

        // Add metadata to PecService

        for (Descriptor descriptor : metadata) {
            localData.put(descriptor, appId);
            dataStore.addMetadata(descriptor);
        }

        // Check if there are any lingering queries requesting for the metadata. If positive, send
        // responses to provide metadata.
        ArrayList<Descriptor> irredundantMetadata = lingeringQueryTable.getIrredundantMetadata(metadata);
        ArrayList<Integer> receivers = lingeringQueryTable.getReceiversForMetadata(irredundantMetadata);

        if (irredundantMetadata.isEmpty())
            return;

        int payloadSize = Integer.SIZE;
        for (Descriptor des : irredundantMetadata) {
            payloadSize += Integer.SIZE + des.encodeWire().length;
        }
        ByteBuffer bb = ByteBuffer.allocate(payloadSize);
        bb.putInt(irredundantMetadata.size());
        for (Descriptor des : irredundantMetadata) {
            byte[] desWire = des.encodeWire();
            bb.putInt(desWire.length);
            bb.put(desWire);
        }
        Descriptor descriptor = new Descriptor(Descriptor.DATA_TYPE_METADATA, -1, -1);
        Response response = new Response();
        response.setDescriptor(descriptor);
        response.setPayload(bb.array());

        response.setReceivers(receivers);
        response.setHopNonce(random.nextInt());
        networkAdapter.sendMessage(response, NetworkAdapter.DEFAULT_MAX_RETRANSMIT);
    }

    // Application request data from PecService
    public void appRequestData(Descriptor descriptor, int appId) {

        // multiple chunks requested
        if (descriptor.getChunkAmount() > 1 && descriptor.getChunkId() != -1) {
            for (int i = 0; i < descriptor.getChunkAmount(); ++i) {
                Descriptor subDes = new Descriptor(descriptor);
                subDes.setChunkId(i);
                localQueries.put(subDes, appId);
                Query query = new Query();
                query.setDescriptor(descriptor);
                query.setSender(deviceId);
                query.setHopNonce(random.nextInt());
                networkAdapter.sendMessage(query, 0);
            }
        }
        // only one chunk requested, request that chunk.
        else {
            localQueries.put(descriptor, appId);
            Query query = new Query();
            query.setDescriptor(descriptor);
            query.setSender(deviceId);
            query.setHopNonce(random.nextInt());
            networkAdapter.sendMessage(query, 0);
        }
    }

    // Application request metadata from PecService
    public void appRequestMetadata(int appId) {
        // Provide exist metadata, and add the application to the list of apps that will be notified
        // when new metadata are received in the future.
        context.serviceProvideMetadata(dataStore.getMetadata(), appId);


        // If PDS is not already running, start data discovery
        if (localMetadataQueries.size() == 0) {
            localMetadataQueries.add(appId);
            startDataDiscovery();
        }else{
            localMetadataQueries.add(appId);
        }
    }

    private void startDataDiscovery() {
        pdsMetadataCountDiscovery = 0;
        dataDiscoveryStartRound();
    }

    private void dataDiscoveryStartRound() {
        pdsMetadataCountRound = 0;
        pdsResponseCountRound = 0;
        pdsResponseCountSlot = 0;
        pdsResponseCountWindow = 0;
        pdsWindow.clear();

        Descriptor descriptor = new Descriptor(Descriptor.DATA_TYPE_METADATA, -1, -1);
        ArrayList<Descriptor> metadata = dataStore.getMetadata();
        BloomFilter<Descriptor> bloomFilter = BloomFilter.createBloomFilter(BF_MIN_SIZE, BF_MAX_SIZE, metadata.size(), BF_FPP);

        Query query = new Query();
        query.setDescriptor(descriptor);
        query.setBloomFilter(bloomFilter);
        query.setSender(deviceId);
        query.setHopNonce(random.nextInt());
        networkAdapter.sendMessage(query, 0);

        DataDiscoveryNextSlotTask nextSlotTask = new DataDiscoveryNextSlotTask();
        timer.schedule(nextSlotTask, PDS_SLOT_SIZE);
    }

    class DataDiscoveryNextSlotTask extends TimerTask {
        @Override
        public void run() {
            // update statistics
            if (pdsWindow.size() == PDS_WINDOW_SIZE) {
                pdsResponseCountWindow -= pdsWindow.poll();
            }
            pdsResponseCountWindow += pdsResponseCountSlot;
            pdsWindow.add(pdsResponseCountSlot);

            // decide whether current round is finished, and if it is whether to start next round
            if (pdsWindow.size() == PDS_WINDOW_SIZE &&
                    (pdsResponseCountWindow == 0
                            || pdsResponseCountWindow / pdsResponseCountRound <= PDS_ROUND_FINISH_THRESHOLD)) {
                // round finished
                if (pdsMetadataCountRound == 0
                        || pdsMetadataCountRound / pdsMetadataCountDiscovery <= PDS_DISCOVERY_FINISH_THRESHOLD) {
                    // data discovery finished
                    localMetadataQueries.clear();
                } else {
                    // data discovery not finished
                    dataDiscoveryStartRound();
                }
            } else {
                // round not finished
                pdsResponseCountSlot = 0;
                DataDiscoveryNextSlotTask nextSlotTask = new DataDiscoveryNextSlotTask();
                timer.schedule(nextSlotTask, PDS_SLOT_SIZE);
            }
        }
    }
}
