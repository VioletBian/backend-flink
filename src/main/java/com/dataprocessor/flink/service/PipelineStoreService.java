package com.dataprocessor.flink.service;

import java.time.Instant;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.dataprocessor.flink.config.BackendFlinkProperties;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;

@Service
public class PipelineStoreService {

    private final MongoTemplate mongoTemplate;
    private final BackendFlinkProperties properties;

    public PipelineStoreService(MongoTemplate mongoTemplate, BackendFlinkProperties properties) {
        this.mongoTemplate = mongoTemplate;
        this.properties = properties;
    }

    public Map<String, Object> savePipeline(String name, List<Map<String, Object>> canonicalPipeline) {
        Document document = new Document();
        document.put("name", name);
        document.put("pipeline", canonicalPipeline);
        document.put("create_time", Date.from(Instant.now()));

        Document inserted = mongoTemplate.insert(document, properties.getMongoCollectionName());

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("message", "Pipeline saved successfully");
        response.put("name", name);
        response.put("version_time", toIsoString(inserted.get("create_time")));
        response.put("id", objectIdToString(inserted.get("_id")));
        return response;
    }

    public Map<String, Object> getLatestPipeline(String name) {
        Query query = new Query(Criteria.where("name").is(name))
            .with(Sort.by(Sort.Order.desc("create_time"), Sort.Order.desc("_id")))
            .limit(1);

        Document document = mongoTemplate.findOne(query, Document.class, properties.getMongoCollectionName());
        if (document == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Pipeline `" + name + "` not found.");
        }

        Map<String, Object> response = new LinkedHashMap<>(document);
        response.remove("_id");
        response.put("create_time", toIsoString(response.get("create_time")));
        return response;
    }

    public Map<String, Object> getPipelineVersions(String name, int limit) {
        Query query = new Query(Criteria.where("name").is(name))
            .with(Sort.by(Sort.Order.desc("create_time"), Sort.Order.desc("_id")))
            .limit(limit);

        List<Document> documents = mongoTemplate.find(query, Document.class, properties.getMongoCollectionName());
        if (documents.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Pipeline `" + name + "` not found.");
        }

        List<Map<String, Object>> versions = documents.stream().map(this::toVersionResponse).toList();
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("name", name);
        response.put("versions", versions);
        response.put("total", versions.size());
        return response;
    }

    private Map<String, Object> toVersionResponse(Document document) {
        Map<String, Object> response = new LinkedHashMap<>(document);
        response.put("_id", objectIdToString(response.get("_id")));
        response.put("create_time", toIsoString(response.get("create_time")));
        return response;
    }

    private String toIsoString(Object rawValue) {
        if (rawValue instanceof Date dateValue) {
            return dateValue.toInstant().toString();
        }
        if (rawValue instanceof Instant instantValue) {
            return instantValue.toString();
        }
        return String.valueOf(rawValue);
    }

    private String objectIdToString(Object rawId) {
        if (rawId instanceof ObjectId objectId) {
            return objectId.toHexString();
        }
        return String.valueOf(rawId);
    }
}
