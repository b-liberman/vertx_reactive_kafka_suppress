package boris.stream.suppress.result;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.kafka.clients.producer.ProducerConfig;
import io.reactivex.Single;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.kafka.client.producer.KafkaProducer;
import io.vertx.reactivex.kafka.client.producer.KafkaProducerRecord;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaProducerVerticle extends AbstractVerticle {

  static final String TOPIC = "test";
  static final String VALUE = "value";
  static final String CATEGORY = "category";

  @Override
  public void start(Future<Void> startFuture) throws Exception {

    Single.fromCallable(() -> getProducerProperties())
        .flatMap(config -> Single.just(KafkaProducer.<String, String>create(vertx, config)))
        .subscribe(producer -> {
          final Random random = new Random();
          vertx.setPeriodic(1000, l -> {
            String value = "message_" + random.nextInt();
            Single.just(value)
                .flatMap(v -> Single.just(new JsonObject()
                    .put(CATEGORY, random.nextBoolean() ? "one" : "two").put(VALUE, v)))
                .flatMap(json -> Single.just(KafkaProducerRecord.<String, String>create(TOPIC,
                    "k" + random.nextInt(), json.toString())))
                .subscribe(record -> {
                  log.info(record.value());
                  producer.send(record, done -> {
                    if (done.succeeded()) {
                      log.info("success {}", done.result().getOffset());
                    } else {
                      log.error("error {}", done.cause().getMessage());
                    }
                  });
                });
          });
          startFuture.complete();
        });
  }

  private Map<String, String> getProducerProperties() {
    Map<String, String> config = new HashMap<>();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.StringSerializer.class.getName());
    config.put(ProducerConfig.ACKS_CONFIG, "1");
    return config;
  }
}
