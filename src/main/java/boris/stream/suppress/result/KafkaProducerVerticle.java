package boris.stream.suppress.result;

import java.time.Instant;
import java.util.Map;
import java.util.Random;

import org.apache.kafka.clients.producer.ProducerConfig;

import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.kafka.client.producer.KafkaProducer;
import io.vertx.reactivex.kafka.client.producer.KafkaProducerRecord;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaProducerVerticle extends AbstractVerticle {

  static final String TOPIC = "test";
  static final String VALUE = "value";
  static final String CATEGORY = "category";
  static final String PERIOD = "period";

  private String category;
  private Integer period;

  @Override
  public void init(Vertx vertx, Context context) {
    // TODO Auto-generated method stub
    super.init(vertx, context);
    category = context.config().getString(CATEGORY);
    period = context.config().getInteger(PERIOD);
  }

  @Override
  public void start(Future<Void> startFuture) throws Exception {

    Single.fromCallable(() -> getProducerProperties())
        .flatMap(config -> Single.just(KafkaProducer.<String, String>create(vertx, config))).subscribe(producer -> {
          produceRecords(producer, category, period);
          log.info("producer deployed: {} - {}", category, period);
          super.start(startFuture);
        });
  }

  private void produceRecords(KafkaProducer<String, String> producer, @NonNull String category,
      @NonNull Integer period) {
    final Random random = new Random();
    vertx.setPeriodic(period, l -> {
      Single.just("message_" + random.nextInt())
          .flatMap(v -> Single.just(new JsonObject().put(CATEGORY, category).put(VALUE, v)))
          .flatMap(json -> Single
              .just(KafkaProducerRecord.<String, String>create(TOPIC, "k" + random.nextInt(), json.toString())))
          .subscribeOn(Schedulers.newThread()).subscribe(record -> {
            log.info("{} - {}", record.value(), Instant.now().toEpochMilli());
            producer.send(record, arDone -> {
              if (arDone.succeeded()) {
                log.debug("success {}", arDone.result().getOffset());
              } else {
                log.error("error {}", arDone.cause().getMessage());
              }
            });
          });
    });
  }

  private Map<String, String> getProducerProperties() {

    return Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092", ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer", ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.StringSerializer.class.getName(), ProducerConfig.ACKS_CONFIG, "1");
  }
}
