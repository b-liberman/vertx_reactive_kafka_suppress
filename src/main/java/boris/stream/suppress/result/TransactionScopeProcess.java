package boris.stream.suppress.result;

import static io.vavr.API.$;
import static io.vavr.API.Case;
import static io.vavr.API.Match;
import static io.vavr.Predicates.instanceOf;

import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
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
public class TransactionScopeProcess extends AbstractVerticle {

    static final String MY_ERROR_STATE = "myErrorState";
    private KafkaStreams streams;

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

        StoreBuilder<KeyValueStore<String, String>> keyValueErrorStoreBuilder = Stores
                .keyValueStoreBuilder(Stores.persistentKeyValueStore(MY_ERROR_STATE), Serdes.String(), Serdes.String());
        builder.addStateStore(keyValueErrorStoreBuilder);

        builder.<String, String>stream(KafkaProducerVerticle.TOPIC).process(() -> new Processor<String, String>() {
            private KeyValueStore<String, String> store;
            private boolean firstRecord = true;

            public void init(ProcessorContext context) {
                this.store = (KeyValueStore<String, String>) context.getStateStore(MY_ERROR_STATE);
            }

            public void process(String k, String v) {

                cleanUpForFirstRecord();

                Try.of(() -> invokeExternalApplication(k, v)).recover(exc -> Match(exc)
                        .of(Case($(instanceOf(IllegalStateException.class)), t -> dealWithException(t, k, v, store))))
                        .getOrElseThrow(t -> {
                            store.put("de" + k, "de" + v);
                            return new RuntimeException("doomed again");
                        });
                log.info("--- the current size of the store is " + store.approximateNumEntries());
                // the result should go into a new topic. but cannot do it in the same topology
                // out of the process method
            }

            private void cleanUpForFirstRecord() {
                if (firstRecord) {
                    log.info("--- the initial size of the store is " + store.approximateNumEntries());
                    var iterator = store.all();
                    while (iterator.hasNext()) {
                        var key = iterator.next().key;
                        log.info("---** {} : {} ", key, store.get(key));
                        store.delete(key);
                        log.info("---** ---** ---** ---**");
                    }
                }
                firstRecord = false;
            }

            public void close() {
                // can access this.state
            }
        }, MY_ERROR_STATE);

        return builder;
    }

    private InvResultTS dealWithException(IllegalStateException t, String k, String v,
            KeyValueStore<String, String> store) {
        InvResultTS newInvResult = InvResultTS.builder().key("n" + k).value("n" + v).build();
        store.put(newInvResult.getKey(), newInvResult.getValue());
        return newInvResult;
    }

    private InvResultTS invokeExternalApplication(String k, String v) {
        log.info("GOT KEY {} and VALUE {}", k, v);
        if (new Random().nextDouble() < 0.15) {
            log.info("*!*!*!*!*!* throw a very bad exception!!!");
            throw new RuntimeException("bad exc");
        }
        if (new Random().nextDouble() < 0.35) {
            log.info("****** throw an OK exception!!!");
            throw new IllegalStateException("tr exc");
        }
        log.info("returning OK result");
        return InvResultTS.builder().key(k).value(v).build();
    }

    private KafkaStreams buildAndStartNewStreamsInstance(Properties config, final StreamsBuilder builder) {
        streams = new KafkaStreams(builder.build(), config);
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

@Builder
@Data
class InvResultTS {
    private String key;
    private String value;
}
