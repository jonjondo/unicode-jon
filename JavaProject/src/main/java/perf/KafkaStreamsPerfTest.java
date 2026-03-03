package perf;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaStreamsPerfTest {

    private static final String TOPIC = "merge.clob.md.BINANCE.LINEAR.AllInOne.v1";
    private static final long DURATION_MS = 60_000;
    private static final long REPORT_INTERVAL_MS = 5_000;

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "perf-test-1769668698285");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "18.176.167.150:19092,3.112.214.127:19092,52.68.253.112:19092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);

        props.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "earliest");
        // Consumer tuning for high RTT
        props.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "earliest");
        props.put(StreamsConfig.consumerPrefix("enable.auto.commit"), "false");
//        props.put(StreamsConfig.consumerPrefix("fetch.min.bytes"), 1 * 1024 * 1024);
//        props.put(StreamsConfig.consumerPrefix("fetch.max.wait.ms"),300);
//        props.put(StreamsConfig.consumerPrefix("max.partition.fetch.bytes"), 32 * 1024 * 1024);
//        props.put(StreamsConfig.consumerPrefix("receive.buffer.bytes"), 64 * 1024 * 1024);
//        props.put(StreamsConfig.consumerPrefix("max.poll.interval.ms"), 10000);
        StreamsBuilder builder = new StreamsBuilder();

        AtomicLong msgCount = new AtomicLong();
        AtomicLong byteCount = new AtomicLong();

        long startTimeNs = System.nanoTime();
        long lastReportNs = startTimeNs;
        long lastMsgCount = 0;
        long lastByteCount = 0;

        KStream<byte[], byte[]> stream = builder.stream(TOPIC, Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()));

        stream.foreach((k, v) -> {
            msgCount.incrementAndGet();
            if (v != null) {
                byteCount.addAndGet(v.length);
            }
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        while ((System.nanoTime() - startTimeNs) < DURATION_MS * 1_000_000L) {
            Thread.sleep(REPORT_INTERVAL_MS);
            long nowNs = System.nanoTime();
            long intervalMs = Math.max(1, (nowNs - lastReportNs) / 1_000_000);
            long elapsedMs = (nowNs - startTimeNs) / 1_000_000;

            long curMsgCount = msgCount.get();
            long curByteCount = byteCount.get();
            long deltaMsg = curMsgCount - lastMsgCount;
            long deltaBytes = curByteCount - lastByteCount;

            double intervalSec = intervalMs / 1000.0;
            double intervalMb = deltaBytes / 1024.0 / 1024.0;
            double totalSec = elapsedMs / 1000.0;
            double totalMb = curByteCount / 1024.0 / 1024.0;

            System.out.printf(
                    "elapsed: %.1f s, interval: %.1f s, %.0f msg/s, %.2f MB/s , total msg: %d, total MB: %.2f%n",
                    totalSec,
                    intervalSec,
                    deltaMsg / intervalSec,
                    intervalMb / intervalSec,
                    curMsgCount,
                    totalMb
            );

            lastReportNs = nowNs;
            lastMsgCount = curMsgCount;
            lastByteCount = curByteCount;
        }

        streams.close(Duration.ofSeconds(5));
        double elapsedSec = (System.nanoTime() - startTimeNs) / 1_000_000_000.0;
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
