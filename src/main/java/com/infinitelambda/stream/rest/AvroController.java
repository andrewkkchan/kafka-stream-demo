package com.infinitelambda.stream.rest;

import com.infinitelambda.stream.model.Active;
import com.infinitelambda.stream.model.AvroHttpRequest;
import com.infinitelambda.stream.model.ClientIdentifier;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;

@RestController
@RequiredArgsConstructor
@Slf4j
public class AvroController {

    @PostMapping("/avro/")
    public byte[] createRequest() {
        AvroHttpRequest request = AvroHttpRequest.newBuilder()
                .setRequestTime(Instant.now().toEpochMilli())
                .setClientIdentifierBuilder(ClientIdentifier.newBuilder()
                        .setHostName("google.com")
                        .setIpAddress("171.1.1.0")
                )
                .setActive(Active.YES)
                .build();
        DatumWriter<AvroHttpRequest> writer = new SpecificDatumWriter<>(
                AvroHttpRequest.class);
        byte[] data = new byte[0];
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Encoder jsonEncoder;
        try {
            jsonEncoder = EncoderFactory.get().jsonEncoder(
                    AvroHttpRequest.getClassSchema(), stream);
            writer.write(request, jsonEncoder);
            jsonEncoder.flush();
            data = stream.toByteArray();
        } catch (IOException e) {
            log.error("Serialization error:" + e.getMessage());
        }
        return data;
    }

}
