package boris.stream.suppress.result;

import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import io.reactivex.Flowable;
import io.reactivex.Single;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.control.Try;
import io.vertx.core.Future;
import io.vertx.reactivex.core.AbstractVerticle;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TransactionAndErrorHandlingInStreamsCheckVerticle extends AbstractVerticle {

    static final String MY_ERROR_TOPIC = "myErrorTopic";
    static final String MY_ERROR_STATE = "myErrorState";

    @Override
    public void start(Future<Void> startFuture) throws Exception {

        getStreamConfiguration().map(config -> Tuple.<Properties, StreamsBuilder>of(config, initializeBuilder()))
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

        var branches = builder.<String, String>stream(KafkaProducerVerticle.TOPIC).mapValues(
                (k, v) -> Tuple.<String, Try<InvResult>>of(v, Try.<InvResult>of(() -> invokeExternalApplication(k, v))))
                .branch((k, t2) -> t2._2.isSuccess(), (k, t2) -> t2._2.isFailure());

        // success
        branches[0].mapValues(t2 -> t2._2.get().getValue())
                .peek((k, v) -> log.info("writing to success topic - {} : : {}", k, v)).to("success");

        // failure
        var errorBranches = branches[1].branch((k, t2) -> t2._2.getCause() instanceof IllegalStateException,
                (k, t2) -> true);
        // process illegal state exception
        // errorBranches[0].process(new MyProcessorSupplier(), MY_ERROR_STATE);

        errorBranches[0].<String, String>transform(new ErrorTransformerSupplier()).to(MY_ERROR_TOPIC);
        builder.<String, String>stream(MY_ERROR_TOPIC).process(new ErrorProcessorSupplier());

        // process other exceptions
        errorBranches[1].foreach((k, v) -> {
            log.info("throw the doomed exception");
            throw new IllegalStateException("doomed");
        });

        return builder;
    }

    private InvResult invokeExternalApplication(String k, String v) {
        log.info("GOT KEY {} and VALUE {}", k, v);
        if (new Random().nextDouble() < 0.0001) {
            log.info("*!*!*!*!*!* throw a very bad exception!!!");
            throw new RuntimeException("bad exc");
        }
        if (new Random().nextDouble() < 0.33) {
            log.info("****** throw an OK exception!!!");
            throw new IllegalStateException("tr exc " + System.currentTimeMillis());
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

    private Single<Properties> getStreamConfiguration() {

        return Flowable.fromIterable(List.of(StreamsConfig.APPLICATION_ID_CONFIG, "transaction-error-check",
                StreamsConfig.CLIENT_ID_CONFIG, "t-e-checker1", StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092", StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName(),
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest", StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams",
                StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10, StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0L,
                StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once")).buffer(2)
                .collectInto(new Properties(), (props, entry) -> props.put(entry.get(0), entry.get(1)));
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

class ErrorTransformerSupplier
        implements TransformerSupplier<String, Tuple2<String, Try<InvResult>>, KeyValue<String, String>> {

    static final String EXCEPTION_CAUSE = "exception cause";

    @Override
    public Transformer get() {
        return new Transformer<String, Tuple2<String, Try<InvResult>>, KeyValue<String, String>>() {

            private ProcessorContext context;

            public void init(ProcessorContext context) {
                this.context = context;
            }

            public KeyValue<String, String> transform(String key, Tuple2<String, Try<InvResult>> value) {
                context.headers().add(new RecordHeader(EXCEPTION_CAUSE, value._2.getCause().getMessage().getBytes()));
                return new KeyValue(key, value._1);
            }

            public void close() {
            }
        };
    }

}

@Slf4j
class ErrorProcessorSupplier implements ProcessorSupplier<String, String> {

    @Override
    public Processor<String, String> get() {
        return new Processor<String, String>() {

            private ProcessorContext context;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
            }

            @Override
            public void process(String key, String value) {
                context.headers().headers(ErrorTransformerSupplier.EXCEPTION_CAUSE)
                        .forEach(h -> log.info("got error event - {} : {} : {}", key, value, new String(h.value())));
            }

            @Override
            public void close() {
            }
        };
    }
}