package boris.stream.suppress.result;

import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import io.reactivex.Flowable;
import io.reactivex.Single;
import io.vavr.Tuple2;
import io.vavr.control.Try;
import io.vertx.core.Future;
import io.vertx.reactivex.core.AbstractVerticle;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TransactionAndErrorHandlingInStreamsCheckVerticle extends AbstractVerticle {

    static final String MY_ERROR_STATE = "myErrorState";

    @Override
    public void start(Future<Void> startFuture) throws Exception {

        Single.fromCallable(() -> getStreamConfiguration())
                .map(config -> new Tuple2<Properties, StreamsBuilder>(config, initializeBuilder()))
                .map(t2 -> buildAndStartNewStreamsInstance(t2._1, t2._2)).subscribe(streams -> {
                    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
                    log.info("consumer deployed");
                    startFuture.complete();
                });
    }

    private StreamsBuilder initializeBuilder() {

        var builder = new StreamsBuilder();

        var keyValueErrorStoreBuilder = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(MY_ERROR_STATE),
                Serdes.String(), Serdes.String());
        builder.addStateStore(keyValueErrorStoreBuilder);

        var branches = builder.<String, String>stream(KafkaProducerVerticle.TOPIC)
                .mapValues((k, v) -> new Tuple2<String, Try<InvResult>>(v,
                        Try.<InvResult>of(() -> invokeExternalApplication(k, v))))
                .branch((k, t2) -> t2._2.isSuccess(), (k, t2) -> t2._2.isFailure());

        // success
        branches[0].mapValues(t2 -> t2._2.get().getValue())
                .peek((k, v) -> log.info("writing to success topic - {} : : {}", k, v)).to("success");

        // failure
        var errorBranches = branches[1].branch((k, t2) -> t2._2.getCause() instanceof IllegalStateException,
                (k, t2) -> true);
        // process illegal state exception
        errorBranches[0].process(new MyProcessorSupplier(), MY_ERROR_STATE);
        // process other exceptions
        errorBranches[1].foreach((k, v) -> {
            if (true) {
                log.info("throw the doomed exception");
                throw new IllegalStateException("doomed");
            }
        });

        return builder;
    }

    private InvResult invokeExternalApplication(String k, String v) {
        log.info("GOT KEY {} and VALUE {}", k, v);
        if (new Random().nextDouble() < 0.1) {
            log.info("*!*!*!*!*!* throw a very bad exception!!!");
            throw new RuntimeException("bad exc");
        }
        if (new Random().nextDouble() < 0.33) {
            log.info("****** throw an OK exception!!!");
            throw new IllegalStateException("tr exc");
        }
        log.info("returning OK result");
        return InvResult.builder().key(k).value(v).build();
    }

    private KafkaStreams buildAndStartNewStreamsInstance(Properties config, final StreamsBuilder builder) {
        final var streams = new KafkaStreams(builder.build(), config);
        streams.setUncaughtExceptionHandler((thread, e) -> {
            log.info("caught exception {}", e.getMessage());
            streams.cleanUp();
            log.info("EXITING");
        });
        streams.cleanUp();
        streams.start();
        return streams;
    }

    private Properties getStreamConfiguration() {
        
        return Flowable.fromIterable(List.of(StreamsConfig.APPLICATION_ID_CONFIG, "transaction-error-check",
                StreamsConfig.CLIENT_ID_CONFIG, "t-e-checker1", StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092", StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName(),
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest", StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams",
                StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10, StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0L,
                StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once")).buffer(2)
                .collectInto(new Properties(), (props, entry) -> props.put(entry.get(0), entry.get(1))).blockingGet();
    }
}

@Data
@Builder
class InvResult {
    private String key;
    private String value;
}

@Slf4j
class MyProcessorSupplier implements ProcessorSupplier<String, Tuple2<String, Try<InvResult>>> {

    @Override
    public Processor<String, Tuple2<String, Try<InvResult>>> get() {
        return new Processor<String, Tuple2<String, Try<InvResult>>>() {

            private KeyValueStore<String, String> store;

            @Override
            public void init(ProcessorContext context) {
                this.store = (KeyValueStore<String, String>) context
                        .getStateStore(TransactionAndErrorHandlingInStreamsCheckVerticle.MY_ERROR_STATE);
            }

            @Override
            public void process(String key, Tuple2<String, Try<InvResult>> t2) {
                log.info("storing entry in the error store - {} : {}", key, t2._1);
                // to do: store the header. need serde for the store... or just JSON?
                store.put(key, t2._1);
            }

            @Override
            public void close() {
            }
        };
    }
}
