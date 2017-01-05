package com.example.android.pecvideostreaming;


import android.*;
import android.Manifest;
import android.app.Activity;
import android.app.ProgressDialog;
import android.content.ComponentName;
import android.content.ContentUris;
import android.content.Context;
import android.content.DialogInterface;
import android.content.Intent;
import android.content.ServiceConnection;
import android.content.pm.PackageManager;
import android.database.Cursor;
import android.graphics.ImageFormat;
import android.hardware.Camera;
import android.location.Address;
import android.location.Geocoder;
import android.location.Location;
import android.location.LocationListener;
import android.media.MediaRecorder;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Build;
import android.os.Bundle;
import android.os.Environment;
import android.os.Handler;
import android.os.HandlerThread;
import android.os.IBinder;
import android.os.Message;
import android.os.Messenger;
import android.os.ParcelFileDescriptor;
import android.os.RemoteException;
import android.provider.DocumentsContract;
import android.provider.MediaStore;
import android.provider.Settings;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.support.v4.app.ActivityCompat;
import android.support.v4.content.ContextCompat;
import android.support.v7.app.ActionBar;
import android.support.v7.app.AlertDialog;
import android.text.format.DateFormat;
import android.util.Log;
import android.view.KeyEvent;
import android.view.Surface;
import android.view.SurfaceHolder;
import android.view.SurfaceView;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.TextView;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import com.google.android.gms.location.LocationServices;

import android.support.v7.app.AppCompatActivity;
import android.widget.Toast;
import android.widget.ToggleButton;
import android.widget.VideoView;

import com.example.android.pecvideostreaming.R;
import com.example.android.pecvideostreaming.platform.Descriptor;
import com.example.android.pecvideostreaming.platform.PecService;
import com.google.android.gms.appindexing.AppIndex;
import com.google.android.gms.common.ConnectionResult;
import com.google.android.gms.common.api.GoogleApiClient;

import org.webrtc.MediaStream;
import org.webrtc.PeerConnectionFactory;

import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.Deflater;

import static android.webkit.ConsoleMessage.MessageLevel.LOG;


public class MainActivity extends AppCompatActivity implements
        GoogleApiClient.ConnectionCallbacks,
        GoogleApiClient.OnConnectionFailedListener, ActivityCompat.OnRequestPermissionsResultCallback,LocationListener,SurfaceHolder.Callback {

    private static final int APP_ID = 9876;
    private static final int PICKFILE_REQUEST_CODE = 10;
    private static final int PICKFILE_REQUEST_CODE_META=11;
    private ServiceConnection serviceConnection;
    private Messenger serviceMessenger;
    private Messenger rMessenger;
//    int REQUEST_VIDEO_CAPTURED = 1;
    private Camera mCamera;
    private static final int VIDEO_CAPTURE = 101;
    MediaStream mediaStream;
    int count = 0;
    int j=0;
    String coordinates;
    private static final int MY_PERMISSION_ACCESS_COARSE_LOCATION = 11;
    public static final String CAMERA_PREF = "camera_pref";
    private static final int STATE_PREVIEW = 0;
    ViewGroup.LayoutParams params;
    private double  PREVIEW_SIZE_FACTOR = 1.30;
    private final static int PIXEL_FORMAT = ImageFormat.NV21;
    private Camera.Parameters parameters;

    public GoogleApiClient googleApiClient;
    protected Location mCurrentLocation;
    protected String mLastUpdateTime;
    private TextView addressLabel;
    private TextView locationLabel;
    private int mState = STATE_PREVIEW;
    boolean usecamera = true;
    MediaRecorder recorder;
    private Camera.Size previewSize;
    private static final String TAG = "MainActivity";

    private TextView mCallbackText;
    private boolean mInitSuccesful;
    private ToggleButton mToggleButton ;
    private Button viewDataButton;
    //private Button addMetaButton;
    SurfaceView mSurfaceView;
    SurfaceHolder mHolder;
    private static final String VIDEO_PATH_NAME = "/Pictures/test.3gp";
    Button uploadButton;
    private int[] pixels;

    int i = 0;
    public static final int MY_PERMISSIONS_REQUEST_CAMERA = 100;
    /**
     * ATTENTION: This was auto-generated to implement the App Indexing API.
     * See https://g.co/AppIndexing/AndroidStudio for more information.
     */
    private GoogleApiClient client;
    public static final String LOGTAG = "VIDEOCAPTURE";
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        mCallbackText = (TextView) findViewById(R.id.callbackText);
//        registerButton = (Button) findViewById(R.id.registerButton);
        mToggleButton = (ToggleButton) findViewById(R.id.toggleRecordingButton);
        viewDataButton = (Button) findViewById(R.id.viewData);
       // addMetaButton = (Button)  findViewById(R.id.metaData);
        locationLabel = (TextView) findViewById(R.id.locationLabel);
        addressLabel = (TextView) findViewById(R.id.addressLabel);
        mSurfaceView = (SurfaceView) findViewById(R.id.surfaceView);
        uploadButton = (Button) findViewById(R.id.upload);
        mHolder = mSurfaceView.getHolder();
        mHolder.addCallback(this);
        System.out.println("create case");
        if (ContextCompat.checkSelfPermission(this, Manifest.permission.ACCESS_COARSE_LOCATION) != PackageManager.PERMISSION_GRANTED) {

            ActivityCompat.requestPermissions(this, new String[]{Manifest.permission.ACCESS_COARSE_LOCATION}, MY_PERMISSION_ACCESS_COARSE_LOCATION);
        }

//        if (ContextCompat.checkSelfPermission(this, android.Manifest.permission.CAMERA) != PackageManager.PERMISSION_GRANTED) {
//            if (getFromPref(this, ALLOW_KEY)) {
//                showSettingsAlert();
//            } else if (ContextCompat.checkSelfPermission(this,android.Manifest.permission.CAMERA)
//
//                    != PackageManager.PERMISSION_GRANTED) {
//
//                // Should we show an explanation?
//                if (ActivityCompat.shouldShowRequestPermissionRationale(this,
//                        android.Manifest.permission.CAMERA)) {
//                    showAlert();
//                } else {
//                    // No explanation needed, we can request the permission.
//                    ActivityCompat.requestPermissions(this,
//                            new String[]{android.Manifest.permission.CAMERA},
//                            MY_PERMISSIONS_REQUEST_CAMERA);
//                }
//            }
//        }


        serviceConnection = new ServiceConnection() {
            @Override
            public void onServiceConnected(ComponentName name, IBinder service) {
                Toast.makeText(MainActivity.this, "service connected", Toast.LENGTH_SHORT).show();
               // mCallbackText.append("Service connected.\n");
                serviceMessenger = new Messenger(service);

                // Register as soon as bound to PecService. This gives PecService the messenger to
                // talk to the application.
                Message msg = Message.obtain(null, PecService.APP_MSG_REGISTER_APP, APP_ID, 0, rMessenger);
                try {
                    Toast.makeText(MainActivity.this, "Register to PecService", Toast.LENGTH_SHORT).show();
                    serviceMessenger.send(msg);
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
            }


            @Override
            public void onServiceDisconnected(ComponentName name) {
                mCallbackText.append("Service disconnected.\n");
                serviceMessenger = null;
            }
        };
        System.out.println("create case before intent");
        Intent intent = new Intent(MainActivity.this, PecService.class);
        ComponentName cn = startService(intent);
        if (cn == null) {
            mCallbackText.append("Failed to start PecService.\n");
            return;
        }

        boolean ret = bindService(intent, serviceConnection, Context.BIND_IMPORTANT);
        if (ret) {
            Toast.makeText(MainActivity.this, "Bound to PecService", Toast.LENGTH_SHORT).show();
        } else {
            mCallbackText.append("Failed to bind to PecService.\n");
        }
      //  Toast.makeText(MainActivity.this, "File path:" + uploadFilePath, Toast.LENGTH_SHORT).show();

        rMessenger = new Messenger(new Handler() {
            @Override
            public void handleMessage(Message msg) {
                Bundle bundle;
                switch (msg.what) {

                    // PecService provide data to application.
                    // This is probably because the the application requested the data earlier.
                    case PecService.SRV_MSG_REQUEST_METADATA:
                        Toast.makeText(MainActivity.this, "req meta data", Toast.LENGTH_SHORT).show();
                        break;
                    case PecService.SRV_MSG_PROVIDE_DATA:
                        bundle = msg.getData();
                        count++;
                        bundle.setClassLoader(Descriptor.class.getClassLoader());
                        HashMap<Descriptor, byte[]> map = (HashMap<Descriptor, byte[]>) bundle.getSerializable("data");
                        mCallbackText.setText("");
                        for (Descriptor descriptor : map.keySet()) {
                            byte[] data = map.get(descriptor);
                            mCallbackText.append("Receive data from PecService: Data=" + new String(data) + "\n");
                        }

                        break;

                    // PecService requests data from the application.
                    // In the following example, the application replies all the data requested by
                    // PecService.
                    case PecService.SRV_MSG_REQUEST_DATA:

                        // Get the descriptors of data requested by PecService.
                        Toast.makeText(MainActivity.this, "in provide metadata", Toast.LENGTH_SHORT).show();
                        bundle = msg.getData();
                        bundle.setClassLoader(Descriptor.class.getClassLoader());
                        ArrayList<Descriptor> descriptors = (ArrayList<Descriptor>) bundle.getSerializable("descriptors");
                        for (Descriptor descriptor : descriptors) {
                            mCallbackText.append("PecService requesting data: Descriptor=" + descriptor.toString() + "\n");
                        }

                        // Provide data to PecService.

                        if (serviceMessenger == null) {
                            mCallbackText.append("Service is not connected.\n");
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
                            mCallbackText.append("Provide requested data to PecService.\n");
                        } catch (RemoteException e) {
                            e.printStackTrace();
                            mCallbackText.append("Failed to provide requested data to PecService.\n");
                        }

                        break;

                    // PecService provide metadata to application.
                    case PecService.SRV_MSG_PROVIDE_METADATA:

                        bundle = msg.getData();
                        bundle.setClassLoader(Descriptor.class.getClassLoader());
                        ArrayList<Descriptor> metadata = (ArrayList<Descriptor>) bundle.getSerializable("metadata");
                        for (Descriptor descriptor : metadata) {
                            mCallbackText.append("Receive metadata from PecService: Descriptor=" + descriptor.toString() + "\n");
                        }

                        break;
                }
            }
        });

        // Click this button to bind and register the application to PecService.
//        registerButton.setOnClickListener(new Button.OnClickListener() {
//            @Override
//            public void onClick(View v) {
//                Intent intent = new Intent(MainActivity.this, PecService.class);
//                ComponentName cn = startService(intent);
//                if (cn == null) {
//                    mCallbackText.append("Failed to start PecService.\n");
//                    return;
//                }
//
//                boolean ret = bindService(intent, serviceConnection, Context.BIND_IMPORTANT);
//                if (ret) {
//                    mCallbackText.append("Bound to PecService.\n");
//                } else {
//                    mCallbackText.append("Failed to bind to PecService.\n");
//                }
//            }
//        });
        mToggleButton.setOnClickListener(new View.OnClickListener() {
            public void onClick(View v) {
//                }
                System.out.println("in on click");
                if (((ToggleButton)v).isChecked()){
                    recorder.start();
                    try {
                        mCamera.reconnect();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    Camera.Parameters parameters = mCamera.getParameters();
                    final Camera.Size size = getOptimalSize();
                    parameters = mCamera.getParameters();
                    previewSize = parameters.getPreviewSize();
                    List<Camera.Size> sizes = parameters.getSupportedPictureSizes();
                    Camera.Size mSize = null;
                    for (Camera.Size size1 : sizes) {
                        Log.i(TAG, "Available resolution: "+size.width+" "+size.height);
                        if (wantToUseThisResolution(size1)) {
                            mSize = size;
                            break;
                        }
                    }
                    parameters.setPictureSize(mSize.width, mSize.height);
                    parameters.setPreviewFormat(PIXEL_FORMAT);
                    mCamera.setParameters(parameters);
                    mCamera.setPreviewCallback(new Camera.PreviewCallback() {
                        public void onPreviewFrame(byte[] _data, Camera _camera) {
                            String testdata = "Hello";
                            final HashMap<Descriptor, byte[]> dat = new HashMap<>();
                            final Message msg1 = Message.obtain(null, PecService.APP_MSG_PROVIDE_DATA, APP_ID, 0);

                            Descriptor descriptor = new Descriptor(1, 1, 1);
                            System.out.println("Trasfer data:"+_data.length);
                            try {
                                byte [] transferData= compress(_data);
                                sendData(dat,msg1,descriptor,transferData);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
//                            byte[] data1 = Arrays.copyOfRange(_data,0,_data.length/4);
//                            byte[] data2 = Arrays.copyOfRange(_data,(_data.length/4)+1,(_data.length/2));
//                            byte[] data3 = Arrays.copyOfRange(_data,(_data.length/2)+1,((_data.length/4)*3)+1);
//                            byte[] data4 = Arrays.copyOfRange(_data,((_data.length/4)*3)+1,_data.length);
                            //for(int i=0;i<4;i++){
//                                dat.put(descriptor, data);
//                                Bundle replyBundle = new Bundle();
//                                replyBundle.putSerializable("data", dat);
                                //msg1.setData(replyBundle);
//                                sendData(dat,msg1,descriptor,data1);
//                                sendData(dat,msg1,descriptor,data2);
//                               sendData(dat,msg1,descriptor,data3);
//                               sendData(dat,msg1,descriptor,data4);
//                                try {
//                                    serviceMessenger.send(msg1);
//                                } catch (RemoteException e) {
//                                    e.printStackTrace();
//                                }
                            //}

                        }
                    });
                }  else {
                    recorder.stop();
                    recorder.reset();
                    try {
                        getCurrentLocation();
                        initRecorder(mHolder.getSurface());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

            }
        });
        viewDataButton.setOnClickListener(new View.OnClickListener() {
            public void onClick(View v) {
                Intent in = new Intent(MainActivity.this, ListActivity.class);
                startActivity(in);
            }
        });

        uploadButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                if (serviceMessenger == null) {
                    mCallbackText.append("Service is not connected.\n");
                    return;
                }
                Intent intent = new Intent(Intent.ACTION_GET_CONTENT);
                intent.setType("*/*");
                startActivityForResult(intent, PICKFILE_REQUEST_CODE);
            }
        });

        // ATTENTION: This was auto-generated to implement the App Indexing API.
        // See https://g.co/AppIndexing/AndroidStudio for more information.
        client = new GoogleApiClient.Builder(this).addApi(AppIndex.API).build();
    }

    private void sendData(HashMap<Descriptor, byte[]> dat,Message msg1,Descriptor descriptor,byte[] data){
        dat.put(descriptor, data);
        Bundle replyBundle = new Bundle();
        replyBundle.putSerializable("data", dat);
        msg1.setData(replyBundle);
        try {
            serviceMessenger.send(msg1);
        } catch (RemoteException e) {
            e.printStackTrace();
        }

    }

    public static byte[] compress(byte[] data) throws IOException {
        Deflater deflater = new Deflater(1);
        deflater.setInput(data);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);
        deflater.finish();
        byte[] buffer = new byte[1024];
        while (!deflater.finished()) {
            int count = deflater.deflate(buffer); // returns the generated code... index
            outputStream.write(buffer, 0, count);
        }
        outputStream.close();
        byte[] output = outputStream.toByteArray();
        System.out.println("Original: " + data.length / 1024 + " Kb");
        System.out.println("Compressed: " + output.length / 1024 + " Kb");
        return output;
    }

    public boolean onKeyDown(int keyCode, KeyEvent event) {
        if (keyCode == KeyEvent.KEYCODE_BACK) {
            exitByBackKey();

            //moveTaskToBack(false);

            return true;
        }
        return super.onKeyDown(keyCode, event);
    }

    protected void exitByBackKey() {

        AlertDialog alertbox = new AlertDialog.Builder(this)
                .setMessage("Do you want to exit application?")
                .setPositiveButton("Yes", new DialogInterface.OnClickListener() {

                    // do something when the button is clicked
                    public void onClick(DialogInterface arg0, int arg1) {

                        finish();
                        //close();


                    }
                })
                .setNegativeButton("No", new DialogInterface.OnClickListener() {

                    // do something when the button is clicked
                    public void onClick(DialogInterface arg0, int arg1) {
                    }
                })
                .show();

    }

    public void onRequestPermissionsResult(int requestCode, String permissions[], int[] grantResults) {
        switch (requestCode) {
            case MY_PERMISSION_ACCESS_COARSE_LOCATION: {
                // If request is cancelled, the result arrays are empty.
                if (grantResults.length > 0
                        && grantResults[0] == PackageManager.PERMISSION_GRANTED) {

                } else {

                }
                //return;
            }
        }
    }




    public static void startInstalledAppDetailsActivity(Context mContext) {

        Intent i = new Intent();
        i.setAction(Settings.ACTION_APPLICATION_DETAILS_SETTINGS);
        i.addCategory(Intent.CATEGORY_DEFAULT);
        i.setData(Uri.parse("package:" + mContext.getPackageName()));
        i.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
        i.addFlags(Intent.FLAG_ACTIVITY_NO_HISTORY);
        i.addFlags(Intent.FLAG_ACTIVITY_EXCLUDE_FROM_RECENTS);
        mContext.startActivity(i);
    }


    private void showSettingsAlert() {
        AlertDialog alertDialog = new AlertDialog.Builder(MainActivity.this).create();
        alertDialog.setTitle("Alert");
        alertDialog.setMessage("App needs to access the Camera.");

        alertDialog.setButton(AlertDialog.BUTTON_NEGATIVE, "DONT ALLOW",
                new DialogInterface.OnClickListener() {

                    public void onClick(DialogInterface dialog, int which) {
                        dialog.dismiss();
                        //finish();
                    }
                });

        alertDialog.setButton(AlertDialog.BUTTON_POSITIVE, "SETTINGS",
                new DialogInterface.OnClickListener() {

                    public void onClick(DialogInterface dialog, int which) {
                        dialog.dismiss();
                        startInstalledAppDetailsActivity(MainActivity.this);
                    }
                });

        alertDialog.show();
    }

    /* Init the MediaRecorder, the order the methods are called is vital to
        * its correct functioning */
    private void initRecorder(Surface surface) throws IOException {
        // It is very important to unlock the camera before doing setCamera
        // or it will results in a black preview
        if(mCamera == null) {
            try{
                mCamera = Camera.open();
            }catch (Exception e){
                Log.d("ERROR", "Failed to get camera: " + e.getMessage());
            }
            parameters = mCamera.getParameters();
            previewSize = parameters.getPreviewSize();
            List<Camera.Size> sizes = parameters.getSupportedPictureSizes();
            Camera.Size mSize = null;
            for (Camera.Size size : sizes) {
                Log.i(TAG, "Available resolution: "+size.width+" "+size.height);
                if (wantToUseThisResolution(size)) {
                    mSize = size;
                    break;
                }
            }
            parameters.setPictureSize(mSize.width, mSize.height);
            mCamera.setParameters(parameters);
            pixels = new int[previewSize.width*  previewSize.height];
            mCamera.unlock();

        }
        if(recorder == null)
            recorder = new MediaRecorder();
        recorder.setPreviewDisplay(surface);


        recorder.setCamera(mCamera);
        recorder.setVideoSource(MediaRecorder.VideoSource.CAMERA);
        recorder.setOutputFormat(MediaRecorder.OutputFormat.DEFAULT);
        File file = new File(Environment.getExternalStorageDirectory(),VIDEO_PATH_NAME);
        if(!file.exists()) {
            File parent = file.getParentFile();
            if(parent != null)
                if(!parent.exists())
                    if(!parent.mkdirs())
                        throw new IOException("Cannot create " +
                                "parent directories for file: " + file);

            file.createNewFile();
        }

        recorder.setOutputFile(file.getAbsolutePath());

        recorder.setMaxDuration(-1);
        recorder.setVideoSize(320,240);
        recorder.setVideoFrameRate(15);

        recorder.setVideoEncoder(MediaRecorder.VideoEncoder.DEFAULT);

        try {
            recorder.prepare();

        }catch (IllegalStateException e) {
            e.printStackTrace();
            finish();
        } catch (IOException e) {
            e.printStackTrace();
            finish();
        }

        mInitSuccesful = true;
    }


    public  boolean wantToUseThisResolution(Camera.Size size){
        if(size.width==176 && size.height==144 ){
            return true;
        }
        return false;
    }

    public static byte[] readFile(File file) throws IOException {
        // Open file
        RandomAccessFile f = new RandomAccessFile(file, "r");
        try {
            long longlength = f.length();
            int length = (int) longlength;
            if (length != longlength)
                throw new IOException("File size is very big");
            byte[] data = new byte[length];
            f.readFully(data);
            return data;
        } finally {
            f.close();
        }
    }


    @Override
    public void surfaceCreated(SurfaceHolder holder) {
        try {
            if(!mInitSuccesful)
                getCurrentLocation();
                initRecorder(mHolder.getSurface());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void surfaceDestroyed(SurfaceHolder holder) {

        shutdown();
    }


void decodeYUV420SP(int[] rgb, byte[] yuv420sp, int width, int height) {

    final int frameSize = width * height;

    for (int j = 0, yp = 0; j < height; j++) {       int uvp = frameSize + (j >> 1) * width, u = 0, v = 0;
        for (int i = 0; i < width; i++, yp++) {
            int y = (0xff & ((int) yuv420sp[yp])) - 16;
            if (y < 0)
                y = 0;
            if ((i & 1) == 0) {
                v = (0xff & yuv420sp[uvp++]) - 128;
                u = (0xff & yuv420sp[uvp++]) - 128;
            }

            int y1192 = 1192 * y;
            int r = (y1192 + 1634 * v);
            int g = (y1192 - 833 * v - 400 * u);
            int b = (y1192 + 2066 * u);

            if (r < 0)                  r = 0;               else if (r > 262143)
                r = 262143;
            if (g < 0)                  g = 0;               else if (g > 262143)
                g = 262143;
            if (b < 0)                  b = 0;               else if (b > 262143)
                b = 262143;

            rgb[yp] = 0xff000000 | ((r << 6) & 0xff0000) | ((g >> 2) & 0xff00) | ((b >> 10) & 0xff);
        }
    }
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
            mCamera.setPreviewCallback(new Camera.PreviewCallback() {
                                          public void onPreviewFrame(byte[] _data, Camera _camera) {
                                          }
                                      });
            mCamera.setPreviewDisplay(mHolder);
            mCamera.startPreview();

        } catch (Exception e){
            Log.d(TAG, "Error starting camera preview: " + e.getMessage());
        }
    }

    private Camera.Size getOptimalSize() {
        Camera.Size result = null;
        final Camera.Parameters parameters = mCamera.getParameters();
        for (final Camera.Size size : parameters.getSupportedPreviewSizes()) {
            if (size.width <= 320 * PREVIEW_SIZE_FACTOR && size.height <= 240 * PREVIEW_SIZE_FACTOR) {
                if (result == null) {
                    result = size;
                } else {
                    final int resultArea = result.width * result.height;
                    final int newArea = size.width * size.height;

                    if (newArea > resultArea) {
                        result = size;
                    }
                }
            }
        }
        if (result == null) {
            result = parameters.getSupportedPreviewSizes().get(0);
        }
        return result;
    }

    private void shutdown() {
        recorder.reset();
        recorder.release();
        mCamera.release();
        recorder = null;
        mCamera = null;
    }


    private void showAlert() {
        AlertDialog alertDialog = new AlertDialog.Builder(MainActivity.this).create();
        alertDialog.setTitle("Alert");
        alertDialog.setMessage("App needs to access the Camera.");

        alertDialog.setButton(AlertDialog.BUTTON_NEGATIVE, "DONT ALLOW",
                new DialogInterface.OnClickListener() {
                    public void onClick(DialogInterface dialog, int which) {
                        dialog.dismiss();
                        finish();
                    }
                });

        alertDialog.setButton(AlertDialog.BUTTON_POSITIVE, "ALLOW",
                new DialogInterface.OnClickListener() {

                    public void onClick(DialogInterface dialog, int which) {
                        dialog.dismiss();
                        ActivityCompat.requestPermissions(MainActivity.this,
                                new String[]{Manifest.permission.CAMERA},
                                MY_PERMISSIONS_REQUEST_CAMERA);
                    }
                });
        alertDialog.show();
    }

    private void openCamera() throws IOException {
        if (ContextCompat.checkSelfPermission(MainActivity.this, Manifest.permission.CAMERA)
                != PackageManager.PERMISSION_GRANTED) {
            //requestCameraPermission();
            return;
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



    protected void onActivityResult(int requestCode, int resultCode, Intent data) {

        //     if (requestCode == REQUEST_VIDEO_CAPTURED && resultCode == RESULT_OK) {
//            Uri videoUri = data.getData();
        if(data==null){
            return;
        }
        Uri fileUri = data.getData();
        if (fileUri != null){
            String filePath = fileUri.getPath();
            filePath=getPath(this,fileUri);

            File file = new File(filePath);
            String fileName = filePath.substring(filePath.lastIndexOf('/')+1);
        try {

//            int subByteLength = 5000;
//            int size = ((transferData.length-1)/subByteLength)+1;
//            byte[][] newArray = new byte[size][];
//            int to = transferData.length;
//            int cursor = size - 1;
//            int from = cursor * subByteLength;
//            while (cursor >= 0) {
//                newArray[cursor] = Arrays.copyOfRange(transferData, from, to);
//                to = from;
//                from -= subByteLength;
//                cursor --;
//            }


          //  if(requestCode ==PICKFILE_REQUEST_CODE) {
                byte[] filedata = readFile(file);
                byte [] transferData= compress(filedata);
                final HashMap<Descriptor, byte[]> dat = new HashMap<>();
                final Message msg1 = Message.obtain(null, PecService.APP_MSG_PROVIDE_DATA, APP_ID, 0);
                HashMap<String, String> fileMap = new HashMap<>();
                HashMap<String, String> fileMapmeta = new HashMap<>();
                ArrayList<Descriptor> metadata = new ArrayList<>();
                int k =j+3;
                for (; j < k; j++) {
                    Descriptor descriptor1 = new Descriptor(j, 1, 1);
                    dat.put(descriptor1, transferData);
                    //metadata.add(descriptor1);
                 }
                k=j+3;
                for (; j < k; j++) {
            Descriptor descriptor1 = new Descriptor(j, 1, 1);
                    dat.put(descriptor1, fileName.getBytes());
                    //metadata.add(descriptor1);
                }


                // dat.put(descriptor1, transferData);
                Bundle replyBundle = new Bundle();
                replyBundle.putSerializable("data", dat);
                msg1.setData(replyBundle);
                try {
                    serviceMessenger.send(msg1);
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
            //} else if(requestCode ==PICKFILE_REQUEST_CODE_META) {
//                HashMap<String, String> fileMapmeta = new HashMap<>();
//                fileMapmeta.put("fileName", fileName);
//                System.out.println("in file name:"+fileName);
//                ArrayList<Descriptor> metadata = new ArrayList<>();
                k= j+3;
                for (; j < k; ++j) {
                    Descriptor descriptor = new Descriptor(j, 1, 1, fileMapmeta);
                    metadata.add(descriptor);
                }
                Bundle bundle = new Bundle();
                bundle.putSerializable("metadata", metadata);
                Message msg2 = Message.obtain(null, PecService.APP_MSG_PROVIDE_METADATA, APP_ID, 0);
                msg2.setData(bundle);
                try {
                    serviceMessenger.send(msg2);
                    Toast.makeText(MainActivity.this, "Meta data added to service", Toast.LENGTH_SHORT);
                } catch (RemoteException e) {
                    e.printStackTrace();
                    mCallbackText.append("Failed to add test metadata to PecService.\n");
                }
           // }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    }
    public static String getPath(final Context context, final Uri uri) {

        final boolean isKitKat = Build.VERSION.SDK_INT >= Build.VERSION_CODES.KITKAT;

        if (isKitKat && DocumentsContract.isDocumentUri(context, uri)) {
            if (isExternalStorageDocument(uri)) {
                final String docId = DocumentsContract.getDocumentId(uri);
                final String[] split = docId.split(":");
                final String type = split[0];

                if ("primary".equalsIgnoreCase(type)) {
                    return Environment.getExternalStorageDirectory() + "/" + split[1];
                }
            }
            else if (isDownloadsDocument(uri)) {

                final String id = DocumentsContract.getDocumentId(uri);
                final Uri contentUri = ContentUris.withAppendedId(
                        Uri.parse("content://downloads/public_downloads"), Long.valueOf(id));

                return getDataColumn(context, contentUri, null, null);
            }
            else if (isMediaDocument(uri)) {
                final String docId = DocumentsContract.getDocumentId(uri);
                final String[] split = docId.split(":");
                final String type = split[0];

                Uri contentUri = null;
                if ("image".equals(type)) {
                    contentUri = MediaStore.Images.Media.EXTERNAL_CONTENT_URI;
                } else if ("video".equals(type)) {
                    contentUri = MediaStore.Video.Media.EXTERNAL_CONTENT_URI;
                } else if ("audio".equals(type)) {
                    contentUri = MediaStore.Audio.Media.EXTERNAL_CONTENT_URI;
                }

                final String selection = "_id=?";
                final String[] selectionArgs = new String[] {
                        split[1]
                };

                return getDataColumn(context, contentUri, selection, selectionArgs);
            }
        }
        else if ("content".equalsIgnoreCase(uri.getScheme())) {
            return getDataColumn(context, uri, null, null);
        }
        else if ("file".equalsIgnoreCase(uri.getScheme())) {
            return uri.getPath();
        }

        return null;
    }

    @Nullable
    public static String getDataColumn(Context context, Uri uri, String selection,
                                       String[] selectionArgs) {

        Cursor cursor = null;
        final String column = "_data";
        final String[] projection = {
                column
        };

        try {
            cursor = context.getContentResolver().query(uri, projection, selection, selectionArgs,
                    null);
            if (cursor != null && cursor.moveToFirst()) {
                final int column_index = cursor.getColumnIndexOrThrow(column);
                return cursor.getString(column_index);
            }
        } finally {
            if (cursor != null)
                cursor.close();
        }
        return null;
    }

    public static boolean isExternalStorageDocument(Uri uri) {
        return "com.android.externalstorage.documents".equals(uri.getAuthority());
    }

    public static boolean isDownloadsDocument(Uri uri) {
        return "com.android.providers.downloads.documents".equals(uri.getAuthority());
    }

    public static boolean isMediaDocument(Uri uri) {
        return "com.android.providers.media.documents".equals(uri.getAuthority());
    }

    public void getCurrentLocation(){
        // Get the current location's latitude & longitude
        Log.d("locationservice","i am here in location 1");
        if ( Build.VERSION.SDK_INT >= 23 &&
                ContextCompat.checkSelfPermission( this, android.Manifest.permission.ACCESS_FINE_LOCATION ) != PackageManager.PERMISSION_GRANTED &&
                ContextCompat.checkSelfPermission( this, android.Manifest.permission.ACCESS_COARSE_LOCATION) != PackageManager.PERMISSION_GRANTED) {
            return  ;
        }

        Log.d("locationservice","i am here in location 2");
        Location currentLocation = LocationServices.FusedLocationApi.getLastLocation(googleApiClient);

        String msg = "Current Location: " +
                Double.toString(currentLocation.getLatitude()) + "," +
                Double.toString(currentLocation.getLongitude());
        Log.d("locationservice",msg);
        // Display the current location in the UI
        locationLabel.setText(msg);

        // To display the current address in the UI
        (new GetAddressTask(this)).execute(currentLocation);
    }

    private class GetAddressTask extends AsyncTask<Location, Void, String> {
        Context mContext;

        public GetAddressTask(Context context) {
            super();
            mContext = context;
        }

        @Override
        protected void onPostExecute(String address) {
            locationLabel.setText(coordinates);
            addressLabel.setText(address);
        }


        @Override
        protected String doInBackground(Location... params) {

            Geocoder geocoder = new Geocoder(mContext, Locale.getDefault());
            Location loc = params[0];
            List<Address> addresses = null;
            try {
                addresses = geocoder.getFromLocation(loc.getLatitude(), loc.getLongitude(), 1);
                coordinates = "Current Location: " +
                        Double.toString(loc.getLatitude()) + "," +
                        Double.toString(loc.getLongitude());

            } catch (IOException e1) {
                e1.printStackTrace();
                return ("IO Exception trying to get address");
            } catch (IllegalArgumentException e2) {
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

