package ***.common.process.impl.task;

import ***.common.process.impl.exception.CrmBuildFileTaskException;
import ***.common.process.task.CrmBuildFileTask;
import ***.core.logging.LoggableErrorService;
import ***.domain.entity.*;
import ***.internal.outbound.feed.crm.CrmService;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Build file for CRM feed task implementation
 *
 * @author olegs
 */
@Component
@Scope(value = "prototype")
public class CrmBuildFileTaskImpl extends AbstractPersistableTask implements CrmBuildFileTask {

    @Autowired
    private Logger logger;

    @Autowired
    private LoggableErrorService loggableErrorService;

    @Autowired
    private CrmService crmService;

    private BuildFileRequest request;

    public CrmBuildFileTaskImpl() {
        super(TaskDomain.CRM, TaskType.CRM_FEED_FILE_BUILD);
    }

    @Override
    public CrmBuildFileTaskImpl init(Stream<CrmStorageEntity> list, String filePath, int maxAttempts) {
        request = new BuildFileRequest(list.map(item->{
            crmService.remove(item);
            return item;
        }).collect(Collectors.toList()), filePath);
        setMaxAttempt(maxAttempts);
        return this;
    }

    @Override
    public Serializable getIncomingData() {
        return request;
    }

    @Override
    public Serializable getOutgoingData() {
        return (Serializable) request.getEntities();
    }

    @Override
    public void setIncomingData(String json) throws IOException {
        ObjectMapper mapper = messageConverter.getObjectMapper();
        request = mapper.readValue(json, CrmBuildFileTaskImpl.BuildFileRequest.class);
    }

    @Override
    public TaskState processTask() {
        try {
            crmService.buildFile(request.getEntities(),request.getFileName());
        } catch (IOException e) {
            setResponseMessage("Write CRM feed file exception: " + e.getMessage());
            logger.error("Write CRM feed file exception", e);
            loggableErrorService.execute(CrmBuildFileTaskException.create(null, e));
            return TaskState.FAILED;
        }
        return TaskState.DONE;
    }

    @Override
    public String getChannelCode() {
        return null;
    }


    public static class BuildFileRequest implements Serializable {
        private List<CrmStorageEntity> entities;
        private String fileName;

        public BuildFileRequest(){
            //default constructor for JSON deserialization
        }

        public BuildFileRequest(List<CrmStorageEntity> entities, String fileName) {
            this.entities = entities;
            this.fileName = fileName;
        }

        public List<CrmStorageEntity> getEntities() {
            return entities;
        }

        public void setEntities(List<CrmStorageEntity> entities) {
            this.entities = entities;
        }

        public String getFileName() {
            return fileName;
        }

        public void setFileName(String fileName) {
            this.fileName = fileName;
        }
    }

}
