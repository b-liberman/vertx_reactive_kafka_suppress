package boris.stream.suppress.result;

import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import io.reactivex.Flowable;
import io.reactivex.Single;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ErrorHandlingInStreamsCheckVerticle extends AbstractVerticle {

    static final String MY_TRANSFORM_STATE = "myTransformState";

    private KafkaStreams streams;

    @Override
    public void start(Future<Void> startFuture) throws Exception {

        getStreamConfiguration().subscribe(config -> {
            var builder = initializeBuilder();
            streams = buildAndStartNewStreamsInstance(config, builder);
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
            log.info("consumer deployed");
            startFuture.complete();
        });
    }

    private StreamsBuilder initializeBuilder() {
        var builder = new StreamsBuilder();
        StoreBuilder<KeyValueStore<String, String>> keyValueStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(MY_TRANSFORM_STATE), Serdes.String(), Serdes.String());

        builder.addStateStore(keyValueStoreBuilder);

        builder.<String, String>stream(KafkaProducerVerticle.TOPIC)
                .transform(() -> new MyTransformer(), MY_TRANSFORM_STATE)
                .foreach((k, v) -> log.info("GOT KEY {} and VALUE {}", k, v));
        return builder;
    }

    private KafkaStreams buildAndStartNewStreamsInstance(Properties config, final StreamsBuilder builder) {
        var streams = new KafkaStreams(builder.build(), config);
        streams.setUncaughtExceptionHandler((thread, e) -> {
            log.info("caught exception {}", e.getMessage());
            this.streams.cleanUp();
            log.info("restarting");
            log.info("starting new streams app");
            try {
                this.streams = buildAndStartNewStreamsInstance(config, builder);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
        streams.cleanUp();
        streams.start();
        return streams;
    }

    private Single<Properties> getStreamConfiguration() {

        return Flowable.fromIterable(List.of(StreamsConfig.APPLICATION_ID_CONFIG, "error-check",
                StreamsConfig.CLIENT_ID_CONFIG, "error_check", StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName(),
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest", StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams",
                StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10, StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0L))
                .buffer(2).collectInto(new Properties(), (props, entry) -> props.put(entry.get(0), entry.get(1)));
    }
}

@Slf4j
class MyTransformer implements Transformer<String, String, KeyValue<String, String>> {

    private KeyValueStore<String, String> store;
    private Random random = new Random();

    @Override
    public void init(ProcessorContext context) {
        this.store = (KeyValueStore<String, String>) context
                .getStateStore(ErrorHandlingInStreamsCheckVerticle.MY_TRANSFORM_STATE);
    }

    @Override
    public KeyValue<String, String> transform(String key, String value) {
        store.put(key, value);
        var size = store.approximateNumEntries();
        if (size % 10 == 0) {
            var iterator = store.all();
            while (iterator.hasNext()) {
                store.delete(iterator.next().key);
            }
            return new KeyValue<String, String>("doomedk", "doomedv");
        } else if (size % 3 == 0) {
            if (random.nextDouble() < 0.25) {
                log.info("throwing exception");
                throw new IllegalStateException("exc");
            }
            return null;
        } else {
            return new KeyValue<String, String>(key, new JsonObject(value).put("counter", "" + size).toString());
        }
    }

    @Override
    public void close() {

    }

}