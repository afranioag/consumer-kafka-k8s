package com.aag.pocs.consumer;

import com.aag.pocs.dto.PessoaDTO;
import com.aag.pocs.model.Pessoa;
import com.aag.pocs.repository.PessoaRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaConsumerService {

    private final PessoaRepository pessoaRepository;

    /**
     * Consome mensagens do tópico Kafka
     * Valida e salva no banco H2
     */
    @KafkaListener(
            topics = "${kafka.topic.csv-data}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumir(
            @Payload PessoaDTO pessoaDTO,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        log.info("Recebida mensagem - Partition: {}, Offset: {}, Pessoa: {}",
                partition, offset, pessoaDTO.getNome());

        try {
            Pessoa entity = converterParaEntity(pessoaDTO);
            pessoaRepository.save(entity);

            log.info("✅ Pessoa salva com sucesso: {} (ID: {})",
                    entity.getNome(), entity.getId());

            // 4. Confirmar processamento (commit manual)
            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("❌ Erro ao processar mensagem: {}", pessoaDTO.getNome(), e);
            // Não faz acknowledge - mensagem será reprocessada
            throw e;
        }
    }

    /**
     * Converte DTO para Entity
     */
    private Pessoa converterParaEntity(PessoaDTO dto) {
        return Pessoa.builder()
                .nome(dto.getNome())
                .idade(dto.getIdade())
                .cidade(dto.getCidade())
                .messageId(dto.getMessageId())
                .sourceFile(dto.getSourceFile())
                .build();
    }
}