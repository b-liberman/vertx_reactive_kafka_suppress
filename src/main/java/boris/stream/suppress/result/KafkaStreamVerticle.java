package boris.stream.suppress.result;

import java.time.Duration;
import java.util.List;
import java.util.Map;
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
import io.reactivex.Flowable;
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
          .windowedBy(TimeWindows.of(Duration.ofSeconds(4)).grace(Duration.ZERO)).count()
          // .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded())).toStream().foreach((k,
          .suppress(Suppressed.untilTimeLimit(Duration.ofSeconds(4), BufferConfig.unbounded()))
          // convert to stream
          .toStream().foreach((k, v) -> log.info("********* {}: {} - {}: {}", k.key(),
              k.window().start(), k.window().end(), v));

      streams = buildAndStartsNewStreamsInstance(config, builder);
      Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
      restartStreamsPeriodicaly(config, builder, 227_000L);
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

  private void restartStreamsPeriodicaly(Properties config, final StreamsBuilder builder,
      @NonNull Long period) {
    vertx.setPeriodic(period, l -> {
      log.info("restarting streams!!");
      streams.close();
      streams = buildAndStartsNewStreamsInstance(config, builder);
    });
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
