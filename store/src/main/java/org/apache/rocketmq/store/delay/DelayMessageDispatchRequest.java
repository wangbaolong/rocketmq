package org.apache.rocketmq.store.delay;

import org.apache.rocketmq.store.DispatchRequest;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DelayMessageDispatchRequest extends DispatchRequest {

    private TimeUnit timeUnit = TimeUnit.SECONDS;
    private int bodyCRC;
    private int flag;
    private long bornTimestamp;
    private byte[] bornHost;
    private byte[] storeHost;
    private int reconsumeTimes;
    private byte[] body;

    public DelayMessageDispatchRequest(String topic,
                                       int queueId,
                                       long commitLogOffset,
                                       int msgSize,
                                       long tagsCode,
                                       long storeTimestamp,
                                       long consumeQueueOffset,
                                       String keys,
                                       String uniqKey,
                                       int sysFlag,
                                       long preparedTransactionOffset,
                                       Map<String, String> propertiesMap,
                                       int bodyCRC,
                                       int flag,
                                       long bornTimestamp,
                                       byte[] bornHost,
                                       byte[] storeHost,
                                       int reconsumeTimes,
                                       byte[] body) {
        super(topic,
                queueId,
                commitLogOffset,
                msgSize,
                tagsCode,
                storeTimestamp,
                consumeQueueOffset,
                keys,
                uniqKey,
                sysFlag,
                preparedTransactionOffset,
                propertiesMap);

        this.bodyCRC = bodyCRC;
        this.flag = flag;
        this.bornTimestamp = bornTimestamp;
        this.bornHost = bornHost;
        this.storeHost = storeHost;
        this.reconsumeTimes = reconsumeTimes;
        this.body = body;
    }

    public int getBodyCRC() {
        return bodyCRC;
    }

    public int getFlag() {
        return flag;
    }

    public byte[] getBody() {
        return body;
    }

    public long getBornTimestamp() {
        return bornTimestamp;
    }

    public byte[] getBornHost() {
        return bornHost;
    }


    public byte[] getStoreHost() {
        return storeHost;
    }

    public int getReconsumeTimes() {
        return reconsumeTimes;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    @Override
    public String toString() {
        return "DelayMessageDispatchRequest{" +
                "timeUnit=" + timeUnit +
                ", bodyCRC=" + bodyCRC +
                ", flag=" + flag +
                ", bornTimestamp=" + bornTimestamp +
                ", bornHost=" + Arrays.toString(bornHost) +
                ", storeHost=" + Arrays.toString(storeHost) +
                ", reconsumeTimes=" + reconsumeTimes +
                ", body=" + Arrays.toString(body) +
                '}';
    }
}
