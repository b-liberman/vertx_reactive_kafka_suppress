package boris.stream.suppress.result;

import io.vertx.core.Future;
import io.vertx.reactivex.core.AbstractVerticle;

public class WebServerVerticle extends AbstractVerticle {

  @Override
  public void start(Future<Void> startFuture) throws Exception {
    vertx.createHttpServer().requestHandler(req -> {
      req.response()
        .putHeader("content-type", "text/plain")
        .end("Hello from Vert.x 2222!");
    }).listen(8888, http -> {
      if (http.succeeded()) {
        startFuture.complete();
        System.out.println("HTTP server started on port 8888");
      } else {
        startFuture.fail(http.cause());
      }
    });
  }
}
