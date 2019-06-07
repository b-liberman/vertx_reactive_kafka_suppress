package boris.stream.suppress.result;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.AbstractVerticle;

public class MainVerticle extends AbstractVerticle {

  @Override
  public void start(Future<Void> startFuture) throws Exception {

    // sSystem.setProperty("vertx.logger-delegate-factory-class-name", io.vertx.core.logging.)

    vertx.rxDeployVerticle(WebServerVerticle.class.getName())
        .flatMap(l -> vertx.rxDeployVerticle(KafkaStreamVerticle.class.getName()))
        .flatMap(l -> vertx.rxDeployVerticle(KafkaProducerVerticle.class.getName(),
            new DeploymentOptions().setConfig(new JsonObject()
                .put(KafkaProducerVerticle.CATEGORY, "one").put(KafkaProducerVerticle.PERIOD, 2000))
                .setInstances(2)))
//        .flatMap(l -> vertx.rxDeployVerticle(KafkaProducerVerticle.class.getName(),
//            new DeploymentOptions().setConfig(new JsonObject()
//                .put(KafkaProducerVerticle.CATEGORY, "two").put(KafkaProducerVerticle.PERIOD, 600))
//                .setInstances(1)))
//        .flatMap(l -> vertx.rxDeployVerticle(KafkaProducerVerticle.class.getName(),
//            new DeploymentOptions()
//                .setConfig(new JsonObject().put(KafkaProducerVerticle.CATEGORY, "three")
//                    .put(KafkaProducerVerticle.PERIOD, 400))
//                .setInstances(1)))
        .subscribe(s -> super.start(startFuture));
  }
}
