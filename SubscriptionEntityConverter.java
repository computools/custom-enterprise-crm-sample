package ***.internal.outbound.impl.feed.crm;

import ***.domain.entity.CrmStorageEntity;
import ***.domain.entity.SubscriptionEntity;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Mappings;
import org.mapstruct.ReportingPolicy;

import java.time.LocalDateTime;


/**
 * @author olegs
 *         Converter for SubscriptionEntity to CrmStorageEntity.
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface SubscriptionEntityConverter {

    @Mapping(source = "dto.serialNumber.serialNumber", target = "serialNumber")
    @Mapping(source = "dto.channel.code", target = "channel")
    @Mapping(source = "dto.user.firstName", target = "firstName")
    @Mapping(source = "dto.user.lastName", target = "lastName" )
    @Mapping(source = "dto.user.email", target = "email")
    @Mapping(source = "dto.user.phone", target = "phone")
    CrmStorageEntity toCrmStorageEntity(SubscriptionEntity dto, LocalDateTime optInDate);

}
