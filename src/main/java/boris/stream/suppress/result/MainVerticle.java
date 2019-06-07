package boris.stream.suppress.result;

import io.vertx.core.Future;
import io.vertx.reactivex.core.AbstractVerticle;

public class MainVerticle extends AbstractVerticle {

  @Override
  public void start(Future<Void> startFuture) throws Exception {
    
    //sSystem.setProperty("vertx.logger-delegate-factory-class-name", io.vertx.core.logging.)
    
    vertx.rxDeployVerticle(WebServerVerticle.class.getName())
    .flatMap(l -> vertx.rxDeployVerticle(KafkaStreamVerticle.class.getName()))
    .flatMap(l -> vertx.rxDeployVerticle(KafkaProducerVerticle.class.getName()))
    .subscribe(s -> startFuture.complete());
  }
}
