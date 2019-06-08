package boris.stream.suppress.result;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.kstream.TimeWindows;
import io.reactivex.Single;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaStreamVerticle extends AbstractVerticle {

  private KafkaStreams streams;

  @Override
  public void start(Future<Void> startFuture) throws Exception {

    Single.fromCallable(() -> getStreamConfiguration()).subscribe(config -> {

      final StreamsBuilder builder = new StreamsBuilder();

      builder.<String, String>stream(KafkaProducerVerticle.TOPIC)
          .flatMapValues((k, v) -> List.<JsonObject>of(new JsonObject(v).put("origKey", k)))
          .selectKey((k, v) -> v.getString(KafkaProducerVerticle.CATEGORY))
          .flatMapValues(v -> List.<String>of(v.toString()))
          .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
          .windowedBy(TimeWindows.of(Duration.ofSeconds(5)).grace(Duration.ZERO)).count()
          .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded())).toStream().foreach((k,
              v) -> log.info("{}: {} - {}: {}", k.key(), k.window().start(), k.window().end(), v));

      streams = buildAndStartsNewStreamsInstance(config, builder);
      Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
      restartStreamsPeriodicalls(config, builder, 60_000L);
      log.info("consumer deployed");
      startFuture.complete();
    });
  }

  private KafkaStreams buildAndStartsNewStreamsInstance(Properties config,
      final StreamsBuilder builder) {
    KafkaStreams streams = new KafkaStreams(builder.build(), config);
    streams.cleanUp();
    streams.start();
    return streams;
  }

  private void restartStreamsPeriodicalls(Properties config, final StreamsBuilder builder,
      @NonNull Long period) {
    vertx.setPeriodic(period, l -> {
      log.info("restarting streams!!");
      streams.close();
      streams = buildAndStartsNewStreamsInstance(config, builder);
    });
  }

  private Properties getStreamConfiguration() {
    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "suppress-example");
    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "suppress-client");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.String().getClass().getName());
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10);
    streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0L);
    return streamsConfiguration;
  }
}
