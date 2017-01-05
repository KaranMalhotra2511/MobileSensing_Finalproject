package com.example.android.pecvideostreaming.platform;

/**
 * Created by user on 27-10-2016.
 */

        import android.app.Service;
        import android.content.Intent;
        import android.os.Bundle;
        import android.os.Handler;
        import android.os.IBinder;
        import android.os.Message;
        import android.os.Messenger;
        import android.os.RemoteException;
        import android.provider.Settings;
        import java.math.BigInteger;

        import  com.example.android.pecvideostreaming.core.Forwarder;

        import java.util.ArrayList;
        import java.util.HashMap;
        import java.util.Map;


public class PecService extends Service {

    public static final int APP_MSG_REGISTER_APP = 1;
    public static final int APP_MSG_REQUEST_METADATA = 2;
    public static final int SRV_MSG_PROVIDE_METADATA = 3;
    public static final int SRV_MSG_REQUEST_METADATA = 4;
    public static final int APP_MSG_PROVIDE_METADATA = 5;
    public static final int SRV_MSG_REQUEST_DATA = 6;
    public static final int APP_MSG_PROVIDE_DATA = 7;
    public static final int APP_MSG_REQUEST_DATA = 8;
    public static final int SRV_MSG_PROVIDE_DATA = 9;

    private Messenger rMessenger;   // This is the messenger that PecService uses to receive
    // messages from applications
    private Map<Integer, Messenger> registeredApps; // These are messengers that bound applications use to
    // receive messages from PecService
    private Forwarder forwarder;

    @Override
    public void onCreate() {
        super.onCreate();
        registeredApps = new HashMap<>();
        rMessenger = new Messenger(new Handler() {
            @Override
            public void handleMessage(Message msg) {
                onReceiveAppMessage(msg);
            }
        });

        String s = Settings.Secure.getString(getContentResolver(),
                Settings.Secure.ANDROID_ID);
        BigInteger bi = new BigInteger(s, 16);
        int deviceId = bi.intValue();
        forwarder = new Forwarder(deviceId, this);
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        PecLog.i("PecService is started.");

        forwarder.startListening();
        return START_STICKY;
    }

    @Override
    public IBinder onBind(Intent intent) {
        PecLog.i("An application binds to PecService.");
        return rMessenger.getBinder();
    }


    private void onReceiveAppMessage(Message msg) {
        Bundle bundle;
        switch (msg.what) {

            // Application registers to PecService.
            case APP_MSG_REGISTER_APP:
                PecLog.i("An application registers to PecService. ID: " + msg.arg1);
                registeredApps.put(msg.arg1, (Messenger) msg.obj);
                break;

            // Application requests metadata.
            case APP_MSG_REQUEST_METADATA:
                PecLog.i("App request metadata.");
                forwarder.appRequestMetadata(msg.arg1);
                break;

            // Application provides metadata.
            case APP_MSG_PROVIDE_METADATA:
                bundle = msg.getData();
                bundle.setClassLoader(Descriptor.class.getClassLoader());
                ArrayList<Descriptor> metadata = (ArrayList<Descriptor>) bundle.getSerializable("metadata");
                forwarder.appProvideMetadata(metadata, msg.arg1);
                for (Descriptor descriptor : metadata) {
                    PecLog.i("Add new metadata. Descriptor=" + descriptor.toString());
                }
                break;

            // Application provides data.
            case APP_MSG_PROVIDE_DATA:
                bundle = msg.getData();
                bundle.setClassLoader(Descriptor.class.getClassLoader());
                HashMap<Descriptor, byte[]> map = (HashMap<Descriptor, byte[]>) bundle.getSerializable("data");
                for (Descriptor descriptor : map.keySet()) {
                    byte[] data = map.get(descriptor);
                    forwarder.appProvideData(descriptor, data, msg.arg1);
                    PecLog.i("Add new data. Descriptor=" + descriptor.toString() + ", size=" + data.length);
                }
                break;

            // Application requests data.
            case APP_MSG_REQUEST_DATA:
                bundle = msg.getData();
                bundle.setClassLoader(Descriptor.class.getClassLoader());
                ArrayList<Descriptor> descriptors = (ArrayList<Descriptor>) bundle.getSerializable("descriptors");
                for (Descriptor descriptor : descriptors) {
                    PecLog.i("App request data. Descriptor=" + descriptor.toString());
                    forwarder.appRequestData(descriptor, msg.arg1);
                }
                break;
        }
    }

    // PecService requests data from the application.
    // This is probably because PecService received queries requesting for data that the application
    // claimed it has by providing corresponding metadata.
    public void serviceRequestData (Descriptor descriptor, int appId) {
        Messenger messenger = registeredApps.get(appId);
        Message msg = Message.obtain(null, PecService.SRV_MSG_REQUEST_DATA);

        ArrayList<Descriptor> descriptors = new ArrayList<>();
        descriptors.add(descriptor);

        Bundle bundle = new Bundle();
        bundle.putSerializable("descriptors", descriptors);

        msg.setData(bundle);
        try {
            messenger.send(msg);
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }

    // PecService provides metadata to the application.
    public void serviceProvideMetadata (ArrayList<Descriptor> metadata, int appId) {
        Messenger messenger = registeredApps.get(appId);
        Message msg = Message.obtain(null, PecService.SRV_MSG_PROVIDE_METADATA);

        Bundle bundle = new Bundle();
        bundle.putSerializable("metadata", metadata);

        msg.setData(bundle);
        try {
            messenger.send(msg);
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }


    // PecService provides data to the application.
    // This is probably because PecService received data from the network that are requested by the
    // application earlier.
    public void serviceProvideData (Descriptor descriptor, byte[] data, int appId) {
        Messenger messenger = registeredApps.get(appId);
        Message msg = Message.obtain(null, PecService.SRV_MSG_PROVIDE_DATA);

        HashMap<Descriptor, byte[]> map = new HashMap<>();
        map.put(descriptor, data);

        Bundle bundle = new Bundle();
        bundle.putSerializable("data", map);

        msg.setData(bundle);
        try {
            messenger.send(msg);
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }
}

