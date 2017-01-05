package com.example.android.pecvideostreaming.utils;

/**
 * Created by user on 27-10-2016.
 */

/**
 * Leaky bucket is used to control the rate of sending data from PecService to udp socket.
 */
public class LeakyBucket {

    public static final int RETRY_DELAY = 300;

    private static final int BUCKET_SIZE = 300 * 1024; // byte
    private static final int LEAKY_RATE = 500; // byte/ms

    private long time;
    private int count;

    public LeakyBucket() {
        time = System.currentTimeMillis();
        count = 0;
    }

    /**
     * Estimate time (ms from now) that the given size of packet can be sent out. Return -1 if there
     * is not enough space in the buffer
     */
    public long estimateSendTime(int size) {
        // update state
        long currentTime = System.currentTimeMillis();
        count -= LEAKY_RATE * (currentTime - time);
        if (count < 0)
            count = 0;
        time = currentTime;

        // test the new packet
        if (size > BUCKET_SIZE - count) {
            return -1;
        } else {
            int ret = count / LEAKY_RATE;
            count += size;
            return ret;
        }
    }
}
