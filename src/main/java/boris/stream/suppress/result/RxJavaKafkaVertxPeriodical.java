package boris.stream.suppress.result;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.SingleSource;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecords;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RxJavaKafkaVertxPeriodical extends AbstractVerticle {

  @Data
  @Builder
  static class ErrorCluster {
    private String category;
    private List<JsonObject> errors;
  }

  private static final String ORIG_KEY = "origKey";

  @Override
  public void start(Future<Void> startFuture) throws Exception {

    Single.fromCallable(() -> getConsumerConfiguration()).subscribe(config -> {
      KafkaConsumer<String, String> consumer = KafkaConsumer.<String, String>create(vertx, config);
      consumer.subscribe(KafkaProducerVerticle.TOPIC);

      vertx.setPeriodic(5000, pid -> {
        consumer.rxPoll(0).flatMap(kcrs -> convertToListSingle(kcrs))
            .flatMapPublisher(l -> Flowable.fromIterable(l))
            .groupBy(json -> json.getString(KafkaProducerVerticle.CATEGORY)).subscribe(gf -> {
              gf.collectInto(ErrorCluster.builder().category(gf.getKey())
                  .errors(new ArrayList<JsonObject>()).build(), (ec, jo) -> ec.errors.add(jo))
                  .subscribe(ec -> log.info("{} - {}", ec.category, ec.errors.size()));
            });
      });

      startFuture.complete();
    });
  }

  private SingleSource<? extends List<JsonObject>> convertToListSingle(
      KafkaConsumerRecords<String, String> kcrs) {
    int size = kcrs.size();
    log.info("poll size: {}", size);
    List<JsonObject> jos = new ArrayList<JsonObject>(size);
    ((ConsumerRecords<String, String>) kcrs.getDelegate().records())
        .forEach(r -> jos.add(new JsonObject(r.value())));
    return Single.just(jos);
   }

  private Map<String, String> getConsumerConfiguration() {

    return Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
        ConsumerConfig.GROUP_ID_CONFIG, "rxJavaFlowable",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
  }

}
