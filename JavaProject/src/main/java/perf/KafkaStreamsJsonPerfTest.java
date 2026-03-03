package perf;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaStreamsJsonPerfTest {

    private static final String TOPIC = "merge.clob.md.BINANCE.LINEAR.AllInOne.v1";
    private static final long DURATION_MS = 30_000;
    private static final boolean ENABLE_PARSE_TRANSACTION_TS = true;

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "perf-test-" + System.currentTimeMillis());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "18.176.167.150:19092,3.112.214.127:19092,52.68.253.112:19092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());

        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5000);

        props.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "earliest");

        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1 * 1024 * 1024);  // 1MB
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 64 * 1024 * 1024);  // 50MB
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 10 * 1024 * 1024);  // 10MB per partition
        props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 2 * 1024 * 1024);  // 2MB socket buffer


        StreamsBuilder builder = new StreamsBuilder();

        AtomicLong msgCount = new AtomicLong();
        AtomicLong byteCount = new AtomicLong();
        AtomicLong lastTransactionTs = new AtomicLong();

        long startTime = System.nanoTime();

        KStream<byte[], byte[]> stream = builder.stream(TOPIC, Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()));

        stream.foreach((k, v) -> {
            msgCount.incrementAndGet();
            if (v != null) {
                byteCount.addAndGet(v.length);
                if (ENABLE_PARSE_TRANSACTION_TS) {
                    try {
                        JSONObject obj = JSON.parseObject(new String(v, StandardCharsets.UTF_8));
                        long ts = obj.getLongValue("transaction_ts");
                        lastTransactionTs.set(ts);
                    } catch (Exception ignored) {
                        // Ignore malformed JSON to keep perf test running.
                    }
                }
            }
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Thread.sleep(DURATION_MS);
        streams.close(Duration.ofSeconds(5));
        double elapsedSec = (System.nanoTime() - startTime) / 1_000_000_000.0;
        double mb = byteCount.get() / 1024.0 / 1024.0;

        System.out.println("====== RESULT ======");
        System.out.printf("elapsed:  %.2f s%n", elapsedSec);
        System.out.printf("messages: %d%n", msgCount.get());
        System.out.printf("msg/s:    %.0f%n", msgCount.get() / elapsedSec);
        System.out.printf("MB/s:     %.2f%n", mb / elapsedSec);
        System.out.printf(
                "avg size: %.0f bytes%n",
                msgCount.get() == 0
                        ? 0
                        : (double) byteCount.get() / msgCount.get()
        );
    }

}
