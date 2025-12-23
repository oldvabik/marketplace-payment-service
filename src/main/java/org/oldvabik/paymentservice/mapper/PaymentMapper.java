package org.oldvabik.paymentservice.mapper;

import org.mapstruct.Mapper;
import org.oldvabik.paymentservice.dto.PaymentDto;
import org.oldvabik.paymentservice.entity.Payment;
import java.util.List;

@Mapper(componentModel = "spring")
public interface PaymentMapper {
    PaymentDto toDto(Payment entity);
    List<PaymentDto> toDtoList(List<Payment> entities);
}

