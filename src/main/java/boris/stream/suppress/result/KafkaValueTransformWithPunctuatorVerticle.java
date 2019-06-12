package boris.stream.suppress.result;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaValueTransformWithPunctuatorVerticle extends AbstractVerticle {

  private static final String ERROR_CLUSTER_STORE_STATE = "errorClusterStoreState";
  private KafkaStreams streams;

  @Override
  public void start(Future<Void> startFuture) throws Exception {

    Single.fromCallable(() -> getStreamConfiguration()).subscribe(config -> {

      final StreamsBuilder builder = new StreamsBuilder();

      StoreBuilder<KeyValueStore<String, Long>> keyValueStoreBuilder =
          Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(ERROR_CLUSTER_STORE_STATE),
              Serdes.String(), Serdes.Long());
      // register store
      builder.addStateStore(keyValueStoreBuilder);

      ValueTransformerSupplier<String, Long> valueTransformerSupplier =
          buildValueTransfomerSupplier();

      builder.<String, String>stream(KafkaProducerVerticle.TOPIC)
          .flatMapValues((k, v) -> List.<JsonObject>of(new JsonObject(v).put("origKey", k)))
          .selectKey((k, v) -> v.getString(KafkaProducerVerticle.CATEGORY))
          .flatMapValues(v -> List.<String>of(v.toString()));
//          .transformValues(valueTransformerSupplier, ERROR_CLUSTER_STORE_STATE).toStream()
//          .foreach((k, v) -> log.info("********* {}: {} - {}: {}", k.key(), k.window().start(),
//              k.window().end(), v));

      streams = buildAndStartsNewStreamsInstance(config, builder);
      Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
      log.info("consumer deployed");
      startFuture.complete();
    });
  }

  private ValueTransformerSupplier<String, Long> buildValueTransfomerSupplier() {
    // TODO Auto-generated method stub
    return new ValueTransformerSupplier<String, Long>() {
      public ValueTransformer<String, Long> get() {
        return new ValueTransformer<String, Long>() {
          private StateStore state;

          public void init(ProcessorContext context) {
            this.state = context.getStateStore(ERROR_CLUSTER_STORE_STATE);
            context.schedule(Duration.ofSeconds(5), PunctuationType.WALL_CLOCK_TIME,
                KafkaValueTransformWithPunctuatorVerticle.this::createTickets);
          }

          public void close() {
            // can access this.state
          }

          @Override
          public Long transform(String value) {
            // TODO Auto-generated method stub
            return null;
          }
        };
      }
    };
  }

  public void createTickets(long l) {

  }



  private KafkaStreams buildAndStartsNewStreamsInstance(Properties config,
      final StreamsBuilder builder) {
    KafkaStreams streams = new KafkaStreams(builder.build(), config);
    streams.cleanUp();
    streams.start();
    return streams;
  }

  private Properties getStreamConfiguration() {

    return Flowable
        .fromIterable(List.of(StreamsConfig.APPLICATION_ID_CONFIG, "suppress-example",
            StreamsConfig.CLIENT_ID_CONFIG, "suppress-client",
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
            StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName(),
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest", StreamsConfig.STATE_DIR_CONFIG,
            "/tmp/kafka-streams", StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10,
            StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0L))
        .buffer(2)
        .collectInto(new Properties(), (props, entry) -> props.put(entry.get(0), entry.get(1)))
        .blockingGet();
  }
}
