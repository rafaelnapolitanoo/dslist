package com.rafaelnapolitano.redirectUrlShortener;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class Main implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    private final S3Client s3Client = S3Client.builder().build();

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> input, Context context) {
        String pathParameters= (String) input.get("rawPath");
        String shortUrlCode = pathParameters.replace("/", "");

        if(shortUrlCode == null || shortUrlCode.isEmpty()) {
            throw new IllegalArgumentException("Invalid input: short url required");
        };

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket("url-shortner-storage-rafaelnapolitano")
                .key(shortUrlCode + ".json")
                .build();

        InputStream s3ObjectStream;

        try{
            s3ObjectStream = s3Client.getObject(getObjectRequest);
        } catch (Exception exception){
            throw new RuntimeException("Error featching Url from s3");
        }

        UrlData urlData;
        try{
            urlData = objectMapper.readValue(s3ObjectStream, UrlData.class);

        } catch (Exception e){
            throw new RuntimeException("Error deserializing url data");
        }

        long currentTimeInSecconds = System.currentTimeMillis() / 1000;

        if(currentTimeInSecconds < urlData.getExpirationTime()) {
            Map<String, Object> response = new HashMap<>();
            response.put("statusCode", 302);
            Map<String, String> headers = new HashMap<>();
            headers.put("location", urlData.getOriginalUrl());
            response.put("headers" , headers);
            return response;

        }
        Map<String, Object> response = new HashMap<>();
        response.put("statusCode", 410);
        response.put("body" , "This Url has Expired");
        return response;
    }
}