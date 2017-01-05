package com.example.android.pecvideostreaming.core;

/**
 * Created by user on 27-10-2016.
 */



        import com.example.android.pecvideostreaming.platform.PecLog;
        import com.example.android.pecvideostreaming.utils.LeakyBucket;

        import java.net.DatagramPacket;
        import java.net.DatagramSocket;
        import java.net.InetAddress;
        import java.util.Hashtable;
        import java.util.List;
        import java.util.Timer;
        import java.util.TimerTask;

/**
 * NetworkAdapter handles low-level communication issues.
 *
 * NetworkAdapter works between PEC Forwarder and common socket communication, converting PEC
 * messages to UDP broadcast packets or the other way round. It also deals with one-hop
 * communication issues, including mixed-cast, ack/retransmit and leaky bucket.
 */
public class NetworkAdapter {

    static final int PORT_NUMBER = 7790;
    static final int MAX_PACKET_SIZE = 15000;
    static final long RETRANSMIT_TIMEOUT =  500;
    static final int DEFAULT_MAX_RETRANSMIT = 5;

    Forwarder forwarder;
    int deviceId;

    Thread receivingThread;
    Hashtable <Integer, List<Integer>> waitingAckTable;
    Timer timer;
    LeakyBucket leakyBucket;

    public NetworkAdapter(Forwarder forwarder, int deviceId) {
        this.forwarder = forwarder;
        this.deviceId = deviceId;
        receivingThread = new Thread(new ReceiveMessageThread());
        waitingAckTable = new Hashtable<>();
        timer = new Timer();
        leakyBucket = new LeakyBucket();
    }

    public void startListening(){
        PecLog.i("Start listening to port: " + PORT_NUMBER);
        if(!receivingThread.isAlive()){
            receivingThread.start();
        }
        PecLog.d("deviceid: " + deviceId);
    }

    public void sendMessage(Message msg, int maxRetransmit) {
        long bufferDelay = leakyBucket.estimateSendTime(msg.encodeWire().length);

        if (bufferDelay > 0) {
            Thread sendingThread = new Thread(new SendMessageThread(msg));
            sendingThread.start();

            if (msg.getReceivers().size() > 0 && maxRetransmit > 0) {    // need ack/retransmit
                waitingAckTable.put(msg.getHopNonce(), msg.getReceivers());
                RetransmitTask retransmitTask = new RetransmitTask(msg, maxRetransmit);
                timer.schedule(retransmitTask, RETRANSMIT_TIMEOUT + bufferDelay);
            }
        } else {
            DelaySendingTask delaySendingTask = new DelaySendingTask(msg, maxRetransmit);
            timer.schedule(delaySendingTask, LeakyBucket.RETRY_DELAY);
        }
    }

    private void sendAck(Ack ack){
        sendMessage(ack, 0);
    }

    /**
     * RetransmitTask checks whether ACKs have been received when triggered on timeout (the max time
     * a node should wait for ACKs before retransmit), and retransmit the message if ACKs are not
     * received.
     */
    class RetransmitTask extends TimerTask {
        private Message msg;
        private int maxRetransmit;

        public RetransmitTask(Message msg, int maxRetransmit) {
            this.msg = msg;
            this.maxRetransmit = maxRetransmit;
        }

        @Override
        public void run() {
            if (!waitingAckTable.get(msg.getHopNonce()).isEmpty()) {
                msg.setReceivers(waitingAckTable.get(msg.getHopNonce()));

                long bufferDelay = leakyBucket.estimateSendTime(msg.encodeWire().length);

                if (bufferDelay >= 0) {
                    Thread sendingThread = new Thread(new SendMessageThread(msg));
                    sendingThread.run();

                    maxRetransmit--;
                    if (maxRetransmit > 0) {
                        RetransmitTask retransmitTask = new RetransmitTask(msg, maxRetransmit);
                        timer.schedule(retransmitTask, RETRANSMIT_TIMEOUT + bufferDelay);
                    }
                } else {
                    RetransmitTask retransmitTask = new RetransmitTask(msg, maxRetransmit);
                    timer.schedule(retransmitTask, LeakyBucket.RETRY_DELAY);
                }
            }
        }
    }

    /**
     * DelaySendingTask is used to delay sending of a message when requested by leaky bucket.
     */
    class DelaySendingTask extends TimerTask {
        private Message msg;
        private int maxRetransmit;

        public DelaySendingTask(Message msg, int maxRetransmit) {
            this.msg = msg;
            this.maxRetransmit = maxRetransmit;
        }

        @Override
        public void run() {
            sendMessage(msg, maxRetransmit);
        }
    }

    /**
     * SendPacketThread sends a message through UDP broadcasting in a separate thread.
     */
    class SendMessageThread implements Runnable {

        private final Message msg;

        public SendMessageThread(Message msg) {
            this.msg = msg;
        }

        @Override
        public void run() {
            try {
                byte[] buf = msg.encodeWire();

                PecLog.i("Send message: " + msg.getType() + " " + msg.getNonce() + " " + msg.getHopNonce());

                DatagramSocket socket = new DatagramSocket();
                socket.setBroadcast(true);
                InetAddress address = InetAddress.getByName("255.255.255.255");
                DatagramPacket packet = new DatagramPacket(buf, buf.length, address, PORT_NUMBER);

                socket.send(packet);
                socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * ReceiveMessageThread keeps running and receiving messages from UDP broadcasting.
     */
    class ReceiveMessageThread implements Runnable {
        @Override
        public void run() {
            try {
                DatagramSocket socket = new DatagramSocket(PORT_NUMBER);

                while(true){
                    byte[] buf = new byte[MAX_PACKET_SIZE];
                    DatagramPacket packet = new DatagramPacket(buf, buf.length);
                    socket.receive(packet);

                    byte[] receivedData = packet.getData();

                    switch (Message.peekMessageType(receivedData)){
                        case Query:
                            Query query = new Query(receivedData);
                            PecLog.i("Receive message: Q " + query.getHopNonce());
                            if (query.getReceivers().contains(deviceId)) {
                                Ack ack = new Ack(query.getNonce(), query.getHopNonce(), deviceId);
                                sendAck(ack);
                            }
                            forwarder.onReceiveQuery(query);
                            break;
                        case Response:
                            Response response = new Response(receivedData);
                            PecLog.i("Receive message: R " + response.getHopNonce());
                            if (response.getReceivers().contains(deviceId)) {
                                for (int i : response.getReceivers()) {
                                    PecLog.e(String.valueOf(i));
                                }
                                Ack ack = new Ack(response.getNonce(), response.getHopNonce(), deviceId);
                                sendAck(ack);
                            }
                            forwarder.onReceiveResponse(response);
                            break;
                        case Ack:
                            Ack ack = new Ack(receivedData);
                            PecLog.i("Receive message: A " + ack.getHopNonce());
                            if(waitingAckTable.containsKey(ack.getHopNonce())){
                                List list = waitingAckTable.get(ack.getHopNonce());
                                list.remove(list.indexOf(ack.getFrom()));
                            }
                            break;
                        default:
                            PecLog.w("Unknown type of received message: " + Message.peekMessageType(receivedData));
                            break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}


