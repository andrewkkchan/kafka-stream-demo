package com.infinitelambda.stream.rest;

import com.infinitelambda.stream.model.Active;
import com.infinitelambda.stream.model.AvroHttpRequest;
import com.infinitelambda.stream.model.ClientIdentifier;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

@RestController
@RequiredArgsConstructor
@Slf4j
public class AvroController {

    @PostMapping("/avro/serialize")
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

    @PostMapping("/avro/deserialize")
    public String restoreRequest() {
        JSONObject obj = new JSONObject();
        obj.put("requestTime", 1649084228083L);
        obj.put("active", "YES");
        JSONObject clientIdentifier = new JSONObject();
        clientIdentifier.put("hostName", "google.com");
        clientIdentifier.put("ipAddress", "192.1.1.1");
        obj.put("clientIdentifier", clientIdentifier);
        obj.put("employeeNames", new JSONArray());
        byte[] data = obj.toString().getBytes(StandardCharsets.UTF_8);
        DatumReader<AvroHttpRequest> reader
                = new SpecificDatumReader<>(AvroHttpRequest.class);
        Decoder decoder;
        try {
            decoder = DecoderFactory.get().jsonDecoder(
                    AvroHttpRequest.getClassSchema(), new String(data));
            AvroHttpRequest avroHttpRequest = reader.read(null, decoder);
            return avroHttpRequest.toString();
        } catch (IOException e) {
            log.error("Deserialization error:" + e.getMessage());
            return null;
        }
    }

}
