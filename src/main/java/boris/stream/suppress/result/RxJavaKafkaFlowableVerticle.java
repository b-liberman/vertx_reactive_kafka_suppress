package boris.stream.suppress.result;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.kafka.client.consumer.KafkaConsumer;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RxJavaKafkaFlowableVerticle extends AbstractVerticle {

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
      consumer.toObservable().buffer(5, TimeUnit.SECONDS).subscribe(l -> {
        log.info("buffer size - " + l.size());
        Flowable.fromIterable(l)
            .flatMap(
                record -> Flowable.just(new JsonObject(record.value()).put(ORIG_KEY, record.key())))
            .groupBy(json -> json.getString(KafkaProducerVerticle.CATEGORY)).subscribe(gf -> {
              gf.collectInto(ErrorCluster.builder().category(gf.getKey())
                  .errors(new ArrayList<JsonObject>()).build(), (ec, jo) -> ec.errors.add(jo))
                  .subscribe(ec -> log.info("{} - {}", ec.category, ec.errors.size()));
            });
      });
      
      startFuture.complete();
    });
  }

  private Map<String, String> getConsumerConfiguration() {

    return Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
        ConsumerConfig.GROUP_ID_CONFIG, "rxJavaFlowable",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
  }

}
