package boris.stream.suppress.result;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import io.reactivex.Single;
import io.vertx.core.Future;
import io.vertx.reactivex.core.AbstractVerticle;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AdminClientVerticle extends AbstractVerticle {

    private String groupId;

    @Override
    public void start(Future<Void> startFuture) throws Exception {

        Single.fromCallable(() -> getAdminClientConfiguration()).subscribe(config -> {
            var adminClient = AdminClient.create(config);
            vertx.setPeriodic(2000, pid -> {
                try {
                    adminClient.listConsumerGroups().all().get().stream().forEach(cg -> {
                        groupId = cg.groupId();
                        log.info("---- offsets for {}", groupId);
                        try {
                            adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get()
                                    .entrySet().forEach(entry -> log.info("-------- {} : {} : {}", entry.getKey().topic(),
                                            entry.getKey().partition(), entry.getValue().offset()));
                        } catch (InterruptedException | ExecutionException e) {
                            e.printStackTrace();
                        }
                    });
                    // list topicss
                    adminClient.listTopics().listings().get().stream().forEach(tl -> {
                        log.info("---------------------- got topic {}", tl.name());
                    });

                    log.info("------------------------------------");
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            });
            log.info("admin client created");
            startFuture.complete();
        });
    }

    private Map<String, Object> getAdminClientConfiguration() {
        return Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092", AdminClientConfig.CLIENT_ID_CONFIG,
                "admin_client");
    }
}