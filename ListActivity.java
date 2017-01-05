package com.example.android.pecvideostreaming;

/**
 * Created by user on 27-10-2016.
 */
import android.content.BroadcastReceiver;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.hardware.Camera;
import android.location.Address;
import android.location.Geocoder;
import android.location.Location;
import android.location.LocationListener;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Build;
import android.os.Bundle;
import android.os.Environment;
import android.os.Handler;
import android.os.IBinder;
import android.os.Message;
import android.os.Messenger;
import android.os.RemoteException;
import android.support.v4.app.ActivityCompat;
import android.support.v4.content.ContextCompat;
import android.support.v7.app.ActionBar;
import android.support.v7.app.AppCompatActivity;
import android.text.format.DateFormat;
import android.util.Log;
import android.view.SurfaceHolder;
import android.view.SurfaceView;
import android.view.View;
import android.view.ViewGroup;
import android.widget.RelativeLayout;
import android.widget.TextView;
import android.widget.Toast;
import android.widget.VideoView;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import com.google.android.gms.location.LocationServices;

import com.example.android.pecvideostreaming.platform.Descriptor;
import com.example.android.pecvideostreaming.platform.PecService;
import com.google.android.gms.common.ConnectionResult;
import com.google.android.gms.common.api.GoogleApiClient;

import static android.webkit.ConsoleMessage.MessageLevel.LOG;
import static com.example.android.pecvideostreaming.R.id.addressLabel;


/**
 * Created by user on 13-10-2016.
 */

public class ListActivity extends AppCompatActivity implements SurfaceHolder.Callback,GoogleApiClient.ConnectionCallbacks,
        GoogleApiClient.OnConnectionFailedListener, ActivityCompat.OnRequestPermissionsResultCallback, LocationListener {

    private TextView locationLabel;
    public GoogleApiClient googleApiClient;
    VideoView v1;
    VideoView v2;
    VideoView v3;
    Intent requestIntent;
    private static final int APP_ID = 9875;
    TextView errMsg;
    private  Messenger serviceMessenger;
    private ServiceConnection serviceConnection;
    private Messenger rMessenger;
    private SurfaceHolder mHolder;
    private Camera mCamera;
    private SurfaceView mSurfaceView;
    private boolean mInitSuccesful;
    byte[] fileData =null;
    Set<String> fileNameSet = new HashSet<String>();
    String fileName ="";
    TextView recentView;
    byte [] receiveData=null;
    TextView newText;
    ViewGroup relativeLayout;
    private static final String TAG = "ListActivity";
    protected Location mCurrentLocation;
    protected String mLastUpdateTime;

    protected void onCreate(Bundle savedInstanceState) {

        super.onCreate(savedInstanceState);
        setContentView(R.layout.list_activity_main);
        locationLabel = (TextView) findViewById(R.id.locationLabel);
        v1 = (VideoView) findViewById(R.id.CaptureVideo);
        v2 = (VideoView) findViewById(R.id.CaptureVideo1);
        v3 = (VideoView) findViewById(R.id.CaptureVideo2);
        v1.setVisibility(View.GONE);
        v2.setVisibility(View.GONE);
        v3.setVisibility(View.GONE);
        TextView Loc1 = (TextView) findViewById(R.id.Sample2);
        TextView Loc2 = (TextView) findViewById(R.id.Sample3);
        errMsg = (TextView) findViewById(R.id.callbackText);
        recentView =(TextView) findViewById(R.id.Sample4);
        relativeLayout = (ViewGroup) findViewById(R.id.RelLayoutId);
        Loc1.setOnClickListener(new View.OnClickListener() {
            public void onClick(View v) {
                if (serviceMessenger == null) {
                    errMsg.append("Service is not connected.\n");
                    return;
                }

                ArrayList<Descriptor> descriptors = new ArrayList<>();
                HashMap<String, String> fileMap = new HashMap<>();
                fileMap.put("fileName","hadoop-elephant.jpg");
                for (int i = 0; i < 6; ++i) {
                    Descriptor descriptor = new Descriptor(i, 1, 1);
                    descriptors.add(descriptor);
                }
               //     descriptors.add(descriptor);
                Bundle bundle = new Bundle();
                bundle.putSerializable("descriptors", descriptors);
                Message msg = Message.obtain(null, PecService.APP_MSG_REQUEST_DATA, APP_ID, 0);
                msg.setData(bundle);
                try {
                    serviceMessenger.send(msg);
                    errMsg.setText("");
                    errMsg.append("Requesting data from PecService.\n");
                }catch (Exception e){
                    e.printStackTrace();
                    errMsg.setText("");
                    errMsg.append("Failed to request data to PecService.\n");
                }

            }
        });
        Loc2.setOnClickListener(new View.OnClickListener() {
            public void onClick(View v) {
                if (serviceMessenger == null) {
                    errMsg.append("Service is not connected.\n");
                    return;
                }

                ArrayList<Descriptor> descriptors = new ArrayList<>();
                HashMap<String, String> fileMap = new HashMap<>();
                fileMap.put("fileName","hadoop-elephant.jpg");
                for (int i = 0; i < 6; ++i) {
                    Descriptor descriptor = new Descriptor(i, 1, 1);
                    descriptors.add(descriptor);
                }
                //     descriptors.add(descriptor);
                Bundle bundle = new Bundle();
                bundle.putSerializable("descriptors", descriptors);
                Message msg = Message.obtain(null, PecService.APP_MSG_REQUEST_DATA, APP_ID, 0);
                msg.setData(bundle);
                try {
                    serviceMessenger.send(msg);
                    errMsg.setText("");
                    errMsg.append("Requesting data from PecService.\n");
                }catch (Exception e){
                    e.printStackTrace();
                    errMsg.setText("");
                    errMsg.append("Failed to request data to PecService.\n");
                }

            }
        });

        serviceConnection = new ServiceConnection() {
            @Override
            public void onServiceConnected(ComponentName name, IBinder service) {
                Toast.makeText(ListActivity.this,"service connected",Toast.LENGTH_SHORT).show();
                Toast.makeText(ListActivity.this,"Service connected",Toast.LENGTH_SHORT).show();
//                errMsg.append("Service connected.\n");
                serviceMessenger = new Messenger(service);

                Message msg = Message.obtain(null, PecService.APP_MSG_REGISTER_APP, APP_ID, 0, rMessenger);
                try {
                    Toast.makeText(ListActivity.this,"Register to PecService",Toast.LENGTH_SHORT).show();
                 //   errMsg.append("Register to PecService.\n");
                    serviceMessenger.send(msg);
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
                Message msg1 = Message.obtain(null, PecService.APP_MSG_REQUEST_METADATA, APP_ID, 0, rMessenger);
                try {
                    serviceMessenger.send(msg1);
                    Toast.makeText(ListActivity.this,"Requesting metadata from PecService",Toast.LENGTH_SHORT).show();
                } catch (RemoteException e) {
                    e.printStackTrace();
                    errMsg.append("Failed to request metadata to PecService.\n");
                }
            }

            @Override
            public void onServiceDisconnected(ComponentName name) {
                errMsg.append("Service disconnected.\n");
                serviceMessenger = null;
            }
        };

        Intent intent = new Intent(ListActivity.this, PecService.class);
        ComponentName cn = startService(intent);
        if (cn == null) {
            errMsg.append("Failed to start PecService.\n");
            return;
        }

        boolean ret = bindService(intent, serviceConnection, Context.BIND_IMPORTANT);
        if (ret) {
            Toast.makeText(ListActivity.this,"Bound to PecService",Toast.LENGTH_SHORT).show();
//            errMsg.append("Bound to PecService.\n");
        } else {
            errMsg.append("Failed to bind to PecService.\n");
        }

        rMessenger = new Messenger(new Handler(){
            @Override
            public void handleMessage(Message msg) {
                Bundle bundle;
                switch (msg.what) {

                    // PecService provide data to application.
                    // This is probably because the the application requested the data earlier.
                    case PecService.SRV_MSG_REQUEST_METADATA:
                        Toast.makeText(ListActivity.this, "req meta data", Toast.LENGTH_SHORT).show();
                        break;
                    case PecService.SRV_MSG_PROVIDE_DATA:
                        Toast.makeText(ListActivity.this, "in provide in list activity", Toast.LENGTH_SHORT).show();
                        bundle = msg.getData();
                        bundle.setClassLoader(Descriptor.class.getClassLoader());
                        HashMap<Descriptor, byte[]> map = (HashMap<Descriptor, byte[]>) bundle.getSerializable("data");
                        System.out.println("map:" + map);

                        for (Descriptor descriptor : map.keySet()) {

                            if (descriptor.getDataType() == 5 || descriptor.getDataType() == 4 || descriptor.getDataType() == 3) {
                                byte[] fileNameData = map.get(descriptor);
                                fileName = new String(fileNameData);
                                String extention = fileName.substring(fileName.lastIndexOf('.'));
                                fileName = fileName.substring(0, fileName.lastIndexOf('.')) + extention;
                            } else {
                                fileData = map.get(descriptor);
//                                HashMap<String,String> fileMap = new HashMap<>();
//                                fileMap =descriptor.getAttributes();
                                //fileName = fileMap.get("fileName");
//                                fileNameSet.add(fileMap.get("fileName"));
                                try {
                                    receiveData=decompress(fileData);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                } catch (DataFormatException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
//                            try {
//                                byte [] receiveData=decompress(data);
//                                System.out.println("data:"+receiveData.toString());
//                                System.out.println("data length:"+receiveData.length);
//                                v1.setVideoPath(getDataSource(data));
//                                v1.setVisibility(View.VISIBLE);
//                                v1.start();
//                                v1.requestFocus();
//                            } catch (IOException e) {
//                                e.printStackTrace();
//                            } catch (DataFormatException e) {
//                                e.printStackTrace();
//                            }
//                            try {
//                                System.out.println("Received data:"+data.toString());
//                                v1.setVideoPath(getDataSource(data));
//                                v1.setVisibility(View.VISIBLE);
//                                v1.start();
//                                v1.requestFocus();
//                            } catch (IOException e) {
//                                e.printStackTrace();
//                            }
                        if (!fileName.equals("") && receiveData != null){
                            errMsg.setText("");
//                            errMsg.setText("Received Data");
                            Toast.makeText(ListActivity.this,"Received Data",Toast.LENGTH_SHORT).show();
                            System.out.println("data:" + receiveData.toString());
                            System.out.println("data length:" + fileData.length);
                            System.out.println("file name recieved:" + fileName);
                            try {
                                File file = new File(Environment.getExternalStorageDirectory(), fileName);
                                if(file.exists()){
                                    file.delete();
                                }
                                 File parent = file.getParentFile();
                                    if (parent != null)
                                        if (!parent.exists())
                                            if (!parent.mkdirs())
                                                try {
                                                    throw new IOException("Cannot create " +
                                                            "parent directories for file: " + file);
                                                } catch (IOException e) {
                                                    e.printStackTrace();
                                                }

                                    try {
                                        file.createNewFile();
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                FileOutputStream stream = new FileOutputStream(file.getAbsoluteFile());
                                stream.write(receiveData);
                                stream.close();
                            } catch (FileNotFoundException e1) {
                                e1.printStackTrace();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            fileName="";
                            receiveData=null;
                        }

                        break;

                    case PecService.SRV_MSG_REQUEST_DATA:

                        // Get the descriptors of data requested by PecService.

                        bundle = msg.getData();
                        bundle.setClassLoader(Descriptor.class.getClassLoader());
                        ArrayList<Descriptor> descriptors = (ArrayList<Descriptor>) bundle.getSerializable("descriptors");
                        for (Descriptor descriptor : descriptors) {
                            errMsg.append("PecService requesting data: Descriptor=" + descriptor.toString() + "\n");
                        }

                        // Provide data to PecService.

                        if (serviceMessenger == null) {
                            errMsg.append("Service is not connected.\n");
                            return;
                        }

                        HashMap<Descriptor, byte[]> data = new HashMap<>();
                        String testdata = "TESTDATA_";
                        for (Descriptor descriptor : descriptors) {
                            data.put(descriptor, (testdata + descriptor.getDataType()).getBytes());
                        }

                        Bundle replyBundle = new Bundle();
                        replyBundle.putSerializable("data", data);

                        Message replyMsg = Message.obtain(null, PecService.APP_MSG_PROVIDE_DATA, APP_ID, 0);
                        replyMsg.setData(replyBundle);
                        try {
                            serviceMessenger.send(replyMsg);
                            errMsg.append("Provide requested data to PecService.\n");
                        } catch (RemoteException e) {
                            e.printStackTrace();
                            errMsg.append("Failed to provide requested data to PecService.\n");
                        }

                        break;

                    // PecService provide metadata to application.
                    case PecService.SRV_MSG_PROVIDE_METADATA:
                        Toast.makeText(ListActivity.this,"in provide meta data",Toast.LENGTH_SHORT).show();
                        bundle = msg.getData();
                        bundle.setClassLoader(Descriptor.class.getClassLoader());
                        ArrayList<Descriptor> metadata = (ArrayList<Descriptor>) bundle.getSerializable("metadata");
                        HashMap<String,String> fileMap = new HashMap<>();
                        for (Descriptor descriptor : metadata) {
                            fileMap = descriptor.getAttributes();
                             fileNameSet.add(fileMap.get("fileName"));
                           // errMsg.append("Receive metadata from PecService: Descriptor=" + descriptor.toString() + "\n");
                        }
                        System.out.println("fileMapreceived:"+fileMap);
//                        for(String s: fileNameSet) {
////                            String fileName = fileMap.get("fileName");
//                            String fileName =s;
//                            System.out.println("s :"+s);
//                            newText = new TextView(ListActivity.this);
//                            newText.setId((int) System.currentTimeMillis());
//                            System.out.println("newText.getId():"+newText.getId());
//                            RelativeLayout.LayoutParams layoutParams = new RelativeLayout.LayoutParams(ActionBar.LayoutParams.WRAP_CONTENT, ActionBar.LayoutParams.WRAP_CONTENT);
//                            System.out.println("recentView.getId():"+recentView.getId());
//                            layoutParams.addRule(RelativeLayout.BELOW, recentView.getId());
//                            relativeLayout.addView(newText, layoutParams);
//                            newText.setText(fileName);
//                            final HashMap<String, String> finalFileMap = fileMap;
//                            newText.setOnClickListener(new View.OnClickListener() {
//                                @Override
//                                public void onClick(View v) {
//                                    String str = newText.getText().toString();
//
//                                    if (serviceMessenger == null) {
//                                        errMsg.append("Service is not connected.\n");
//                                        return;
//                                    }
//                                    requestData(finalFileMap.get("fileName"));
//
//                                }
//                            });
//                            recentView = newText;
//                        }
                        break;
                }
            }
        });


    }

    public  void requestData(String fileName){
        if (serviceMessenger == null) {
            errMsg.append("Service is not connected.\n");
            return;
        }

        ArrayList<Descriptor> descriptors = new ArrayList<>();
        HashMap<String, String> fileMap = new HashMap<>();
        fileMap.put("fileName",fileName);
        for (int i = 0; i < 3; ++i) {
            Descriptor descriptor = new Descriptor(i, 1, 1,fileMap);
            descriptors.add(descriptor);
        }
        //     descriptors.add(descriptor);
        Bundle bundle = new Bundle();
        bundle.putSerializable("descriptors", descriptors);
        Message msg = Message.obtain(null, PecService.APP_MSG_REQUEST_DATA, APP_ID, 0);
        msg.setData(bundle);
        try {
            serviceMessenger.send(msg);
            errMsg.setText("");
            //errMsg.append("Requesting data from PecService.\n");
            Toast.makeText(ListActivity.this,"Requesting data from PecService",Toast.LENGTH_SHORT).show();
        }catch (Exception e){
            e.printStackTrace();
            errMsg.setText("");
            errMsg.append("Failed to request data to PecService.\n");
        }
    }

    public static byte[] decompress(byte[] data) throws IOException, DataFormatException {
        Inflater inflater = new Inflater();
        inflater.setInput(data);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);
        byte[] buffer = new byte[1024];
        while (!inflater.finished()) {
            int count = inflater.inflate(buffer);
            outputStream.write(buffer, 0, count);
        }
        outputStream.close();
        byte[] output = outputStream.toByteArray();
        return output;
    }

    private String getDataSource(byte[] bytes) throws IOException {
            InputStream stream = new ByteArrayInputStream(bytes);
            if (stream == null)
                throw new RuntimeException("stream is null");
            File temp = File.createTempFile("mediaplayertmp", "dat");
            temp.deleteOnExit();
            String tempPath = temp.getAbsolutePath();
            FileOutputStream out = new FileOutputStream(temp);
            byte buf[] = new byte[128];
            do {
                int numread = stream.read(buf);
                if (numread <= 0)
                    break;
                out.write(buf, 0, numread);
            } while (true);
            try {
                stream.close();
            } catch (IOException ex) {
                Log.e(TAG, "error: " + ex.getMessage(), ex);
            }
            return tempPath;
    }

    private void ViewVideo(String data){
        Uri myUri = Uri.parse(data);
        System.out.print("in videoView");

        v2.setVisibility(View.VISIBLE);
        v3.setVisibility(View.VISIBLE);
        v1.setVideoURI(myUri);
    }

    @Override
    public void surfaceCreated(SurfaceHolder holder) {
//        try {
//            //if(!mInitSuccesful)
//                //initRecorder(mHolder.getSurface());
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    public void shutdown(){
        mCamera.release();
        mCamera=null;
    }

    @Override
    public void surfaceDestroyed(SurfaceHolder holder) {
        shutdown();
    }

    @Override
    public void surfaceChanged(SurfaceHolder holder, int format, int width,
                               int height) {
        if (mHolder.getSurface() == null){
            return;
        }
        try {
            mCamera.stopPreview();
        } catch (Exception e){
            // ignore: tried to stop a non-existent preview
        }

        try {
            System.out.println("in try");
            mCamera.setPreviewDisplay(mHolder);
            mCamera.startPreview();

        } catch (Exception e){
            Log.d(TAG, "Error starting camera preview: " + e.getMessage());
        }
    }

    @Override
    public void onProviderEnabled(String provider) {

    }

    @Override
    public void onProviderDisabled(String provider) {

    }

    @Override
    public void onLocationChanged(Location location) {
        mCurrentLocation = location;
        mLastUpdateTime = DateFormat.getDateFormat(this).format(new Date());
        Toast.makeText(this, location.toString() + "", Toast.LENGTH_SHORT).show();
    }

    @Override
    public void onConnected(Bundle dataBundle) {
        // Display the connection status
        Toast.makeText(this, "Connected", Toast.LENGTH_SHORT).show();
    }

    @Override
    public void onConnectionSuspended(int cause) {
        // Display the connection status
        Toast.makeText(this, "Disconnected. Please re-connect.", Toast.LENGTH_SHORT).show();
    }

    @Override
    public void onConnectionFailed(ConnectionResult connectionResult) {
        // Display the error code on failure
        Toast.makeText(this, "Connection Failure : " + connectionResult.getErrorCode(), Toast.LENGTH_SHORT).show();
    }

    @Override
    public void onStatusChanged(String provider, int status, Bundle extras) {

    }

    private class GetAddressTask extends AsyncTask<Location, Void, String> {
        Context mContext;

        public GetAddressTask(Context context) {
            super();
            mContext = context;
        }
        @Override
        protected void onPostExecute(String address) {
        }


        @Override
        protected String doInBackground(Location... params) {

            Geocoder geocoder = new Geocoder(mContext, Locale.getDefault());
            Location loc = params[0];
            List<Address> addresses = null;
            try {
                addresses = geocoder.getFromLocation(loc.getLatitude(), loc.getLongitude(), 1);

            } catch (IOException e1) {
                e1.printStackTrace();
                return ("IO Exception trying to get address");
            } catch (IllegalArgumentException e2) {
                // Error message to post in the log
                String errorString = "Illegal arguments " +
                        Double.toString(loc.getLatitude()) + " , " + Double.toString(loc.getLongitude()) + " passed to address service";
                Log.e("LocationSampleActivity", errorString);
                e2.printStackTrace();
                return errorString;
            }
            // If the reverse geocode returned an address
            if (addresses != null && addresses.size() > 0) {
                // Get the first address
                Address address = addresses.get(0);

            /*
            * Format the first line of address (if available),
            * city, and country name.
            */
                String addressText = String.format("%s, %s, %s",

                        // If there's a street address, add it
                        address.getMaxAddressLineIndex() > 0 ?
                                address.getAddressLine(0) : "",
                        // Locality is usually a city
                        address.getSubLocality(),

                        // The country of the address
                        address.getCountryName());

                return addressText;
            } else {
                return "No address found";
            }
        }
    }

}

