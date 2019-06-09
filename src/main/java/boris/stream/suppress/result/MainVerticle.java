package boris.stream.suppress.result;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;
import lombok.NonNull;

public class MainVerticle extends AbstractVerticle {

  @Override
  public void start(Future<Void> startFuture) throws Exception {

    vertx.rxDeployVerticle(WebServerVerticle.class.getName())
        .flatMap(wsvl -> vertx.rxDeployVerticle(KafkaStreamVerticle.class.getName()))
        .flatMap(ksvl -> vertx.rxDeployVerticle(KafkaProducerVerticle.class.getName(),
            buildDeploymentOptions("one", 950, 1)))
//        .flatMap(pvOnel -> vertx.rxDeployVerticle(KafkaProducerVerticle.class.getName(),
//            buildDeploymentOptions("two", 3000, 1)))
//        .flatMap(pvTwol -> vertx.rxDeployVerticle(KafkaProducerVerticle.class.getName(),
//            buildDeploymentOptions("three", 4000, 1)))
        // dummy category to forcefully close the suppress time windows
//      .flatMap(pvTwol -> vertx.rxDeployVerticle(KafkaProducerVerticle.class.getName(),
//      buildDeploymentOptions(KafkaProducerVerticle.DUMMY_CATEGORY, 4000, 1)))
        .subscribe(s -> super.start(startFuture));
  }

  private DeploymentOptions buildDeploymentOptions(@NonNull String category,
      @NonNull Integer period, @NonNull Integer instances) {
    return new DeploymentOptions().setConfig(new JsonObject()
        .put(KafkaProducerVerticle.CATEGORY, category).put(KafkaProducerVerticle.PERIOD, period))
        .setInstances(instances);
  }
}
