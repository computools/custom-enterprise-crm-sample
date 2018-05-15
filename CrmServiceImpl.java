package ***.internal.outbound.impl.feed.crm;


import ***.common.process.impl.scheduling.TriggeredEvent;
import ***.common.process.service.TaskService;
import ***.common.process.task.Task;
import ***.common.service.ConfigService;
import ***.domain.entity.CrmStorageEntity;
import ***.domain.entity.SubscriptionEntity;
import ***.domain.entity.TaskState;
import ***.domain.repository.CrmStorageRepository;
import ***.domain.repository.SubscriptionRepository;
import ***.internal.outbound.feed.crm.CrmService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author olegs
 */
@Service
@Transactional
public class CrmServiceImpl implements CrmService {

    private static final Logger logger = LoggerFactory.getLogger(CrmServiceImpl.class);
    @Autowired
    private ConfigService configService;

    @Autowired
    private ObjectFactory<TriggeredEvent> triggeredEventFactory;

    @Autowired
    private CrmStorageRepository storageRepository;

    @Autowired
    private TaskService taskService;

    @Autowired
    private SubscriptionEntityConverter subscriptionEntityConverter;

    private static final DateTimeFormatter FILE_NAME_FORMATTER = DateTimeFormatter.ofPattern("MMddyyyy");
    private static final DateTimeFormatter OPTIN_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss");
    private static final String FILE_NAME_PATTERN = "IRP_CRM_%s";
    private static final String FILE_EXTENSION_PATTERN = ".txt";
    private static final String COLUMN_SEPARATOR = "\t";
    private static final String ROW_SEPARATOR = "\n";
    private static final String CRON_DEFAULT = "0 0 1 * * *";

    private static final Object buildFileLocker = new Object();

    private ScheduledFuture cronTask;

    @PostConstruct
    public void init() {
        startCron();
    }

    @Override
    public void activation(SubscriptionEntity subscriptionEntity) {
        CrmStorageEntity entity = subscriptionEntityConverter.toCrmStorageEntity(subscriptionEntity,subscriptionEntity.getRegistrationDate().toLocalDateTime());
        if(entity!=null) {
            storageRepository.save(entity);
        }
    }

    @Override
    public void deRegistration(SubscriptionEntity subscriptionEntity) {
        CrmStorageEntity entity = subscriptionEntityConverter.toCrmStorageEntity(subscriptionEntity,subscriptionEntity.getDeregistrationDate());
        if(entity!=null) {
            storageRepository.save(entity);
        }
    }

    @Override
    public void deActivation(SubscriptionEntity subscriptionEntity) {
        CrmStorageEntity entity = subscriptionEntityConverter.toCrmStorageEntity(subscriptionEntity,subscriptionEntity.getDeactivationDate());
        if(entity!=null) {
            storageRepository.save(entity);
        }
    }

    @Override
    public Stream<CrmStorageEntity> get() {
        return storageRepository.findAll().stream();
    }

    @Override
    public void remove(CrmStorageEntity item) {
        storageRepository.delete(item);
    }

    @Override
    public void buildFile(List<CrmStorageEntity> items, String fileName) throws IOException {
        synchronized (buildFileLocker) {
            String path = configService.getFeedCrmPath();
            if (path == null) {
                logger.warn("Parameter service.feed.crm.path don't set. CRM report file will not be write");
                return;
            }

            File folder = new File(path);
            File[] list = folder.listFiles();
            String filePrefix = "";
            if (list != null) {
                int fileCount = Arrays.stream(list).mapToInt(item -> item.getPath().replace("\\", "/").contains(fileName) ? 1 : 0).sum();
                if (fileCount > 0) filePrefix = "_" + fileCount;
            }
            String fullFileName = fileName + filePrefix + FILE_EXTENSION_PATTERN;
            File tmpFile = File.createTempFile(fullFileName, "");
            try (FileWriter writer = new FileWriter(tmpFile)) {
                writer.write(buildColumnsNames() + ROW_SEPARATOR + items.stream().map(this::buildRow).collect(Collectors.joining(ROW_SEPARATOR)));
                writer.flush();
            }
            Files.copy(tmpFile.toPath(), Paths.get(path + File.separatorChar + fullFileName), StandardCopyOption.REPLACE_EXISTING);
            if (!tmpFile.delete()) {
                logger.warn("Can't delete temp file from crm feed report");
            }
        }

    }

    @Override
    public void restartCron() {
        cronTask.cancel(true);
        startCron();
    }

    @Override
    public String fileName() {
        return String.format(FILE_NAME_PATTERN,LocalDateTime.now().format(FILE_NAME_FORMATTER));
    }

    private String buildRow(CrmStorageEntity item) {
        return new StringBuilder().append(buildColumnItem(item.getSerialNumber()))
            .append(buildColumnItem(item.getFirstName())).append(buildColumnItem(item.getLastName()))
            .append(buildColumnItem(item.getEmail())).append(buildColumnItem(item.getPhone()))
            .append(buildColumnItem(item.getChannel())).append(item.isOptIn() ? "Y" : "N").append(COLUMN_SEPARATOR)
            .append(item.getOptInDate()==null?"":item.getOptInDate().format(OPTIN_DATE_FORMATTER)).toString();


    }
    private String buildColumnItem(String data) {
        return (data != null ? data : "") + COLUMN_SEPARATOR;
    }

    private String buildColumnsNames(){
        return new StringBuilder().append(buildColumnItem("serialNumber"))
            .append(buildColumnItem("firstName"))
            .append(buildColumnItem("lastName"))
            .append(buildColumnItem("email"))
            .append(buildColumnItem("phone"))
            .append(buildColumnItem("channel"))
            .append(buildColumnItem("optIn"))
            .append("optInDate").toString();
    }

    private void startCron(){
        logger.info("Start CRM feed export cron");
        String cronRule = configService.getFeedCrmCron();
        if(cronRule == null) cronRule = CRON_DEFAULT;
        cronTask =  triggeredEventFactory.getObject().init(new Task() {
            @Override
            public void submit() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void retry() {
                if(configService.getFeedCrmPath()!= null){
                    taskService.submitCrmBuildFileTask(get(),fileName());
                }
            }

            @Override
            public TaskState call() throws Exception {
                return null;
            }
        },cronRule).schedule();
    }

}
