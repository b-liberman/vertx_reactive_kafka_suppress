package boris.stream.final_.result;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import io.reactivex.Single;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaStreamVerticle extends AbstractVerticle {

  @Override
  public void start(Future<Void> startFuture) throws Exception {

    Single.fromCallable(() -> getStreamConfiguration()).subscribe(config -> {
      StreamsBuilder builder = new StreamsBuilder();
      KStream<String, String> input = builder.stream(KafkaProducerVerticle.TOPIC);

      // input.foreach((k, v) -> log.info("{}:{}", k, v));
      input.flatMapValues((k, v) -> List.<JsonObject>of(new JsonObject(v).put("origKey", k)))
          .selectKey((k, v) -> v.getValue(KafkaProducerVerticle.CATEGORY))
          .flatMapValues(v -> List.<String>of(v.toString())).groupByKey()
          .windowedBy(TimeWindows.of(Duration.ofSeconds(5))).count().toStream()
          .foreach((k, v) -> log.info("{}:{}", k, v));

      KafkaStreams streams = new KafkaStreams(builder.build(), config);
      streams.cleanUp();
      streams.start();
      startFuture.complete();
      Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
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
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
    return streamsConfiguration;
  }
}
