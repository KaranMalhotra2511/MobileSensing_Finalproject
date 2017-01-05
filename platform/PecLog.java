package com.example.android.pecvideostreaming.platform;

/**
 * Created by user on 27-10-2016.
 */


        import android.util.Log;

public class PecLog {
    private static final String TAG = "PecService";

    public static void v(String msg) {
        Log.v(TAG, msg);
    }

    public static void d(String msg) {
        Log.d(TAG, msg);
    }

    public static void i(String msg) {
        Log.i(TAG, msg);
    }

    public static void w(String msg) {
        Log.w(TAG, msg);
    }

    public static void e(String msg) {
        Log.e(TAG, msg);
    }
}