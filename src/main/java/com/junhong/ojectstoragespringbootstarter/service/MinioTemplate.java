package com.junhong.ojectstoragespringbootstarter.service;

import com.junhong.ojectstoragespringbootstarter.connection.MinioConnectionFactory;
import com.junhong.ojectstoragespringbootstarter.exception.MinioBadRequestException;
import io.minio.BucketExistsArgs;
import io.minio.ComposeObjectArgs;
import io.minio.ComposeSource;
import io.minio.GetObjectArgs;
import io.minio.GetPresignedObjectUrlArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.ObjectWriteResponse;
import io.minio.PutObjectArgs;
import io.minio.RemoveBucketArgs;
import io.minio.RemoveObjectArgs;
import io.minio.RemoveObjectsArgs;
import io.minio.Result;
import io.minio.ServerSideEncryption;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import io.minio.http.Method;
import io.minio.messages.Bucket;
import io.minio.messages.DeleteError;
import io.minio.messages.DeleteObject;
import io.minio.messages.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Minio operation template
 *
 * @author jh
 */
public class MinioTemplate {
    private final static Logger log = LoggerFactory.getLogger(MinioTemplate.class);
    private final MinioConnectionFactory minioConnectionFactory;

    public MinioTemplate(MinioConnectionFactory minioConnectionFactory) {
        this.minioConnectionFactory = minioConnectionFactory;
    }

    /**
     * Bucket Operations
     */

    public void createBucket(String bucketName) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        MinioClient client = minioConnectionFactory.getConnection();
        if (!client.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build())) {
            client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
        }
    }

    public List<Bucket> getAllBuckets() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        return minioConnectionFactory.getConnection().listBuckets();
    }

    public Optional<Bucket> getBucket(String bucketName) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        return minioConnectionFactory.getConnection().listBuckets().stream().filter(b -> b.name().equals(bucketName)).findFirst();
    }

    public void removeBucket(String bucketName) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        minioConnectionFactory.getConnection().removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
    }

    public List<Item> getAllObjectsByPrefix(String bucketName, String prefix, boolean recursive) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        List<Item> objectList = new ArrayList();
        Iterable<Result<Item>> objectsIterator = minioConnectionFactory.getConnection().listObjects(ListObjectsArgs.builder().bucket(bucketName).recursive(recursive).prefix(prefix).build());
        for (Result<Item> itemResult : objectsIterator) {
            objectList.add(itemResult.get());
        }
        return objectList;
    }

    /**
     * Object operations
     */
    public InputStream getObject(String bucketName, String objectName) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, ServerException, InternalException, NoSuchAlgorithmException, XmlParserException,  ErrorResponseException, InvalidResponseException {
        return minioConnectionFactory.getConnection().getObject(GetObjectArgs.builder().bucket(bucketName).object(objectName).build());
    }

    public InputStream getObjectVersioned(String bucketName, String objectName, String versionId) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, ServerException, InternalException, NoSuchAlgorithmException, XmlParserException,  ErrorResponseException {
        return minioConnectionFactory.getConnection().getObject(GetObjectArgs.builder().bucket(bucketName).object(objectName).versionId(versionId).build());
    }

    public InputStream getObjectByPart(String bucketName, String objectName, long length, Long offset) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, ServerException, InternalException, NoSuchAlgorithmException, XmlParserException,  ErrorResponseException {
        return minioConnectionFactory.getConnection().getObject(GetObjectArgs.builder().bucket(bucketName).object(objectName).length(length).offset(offset).build());
    }

    public InputStream getObjectWithEncryption(String bucketName, String objectName) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, ServerException, InternalException, NoSuchAlgorithmException, XmlParserException,  ErrorResponseException {
        return minioConnectionFactory.getConnection().getObject(GetObjectArgs.builder().bucket(bucketName).object(objectName).build());
    }

    /**
     * Object operations
     */
    public String getObjectURL(String bucketName, String objectName) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException,  ServerException, InternalException, NoSuchAlgorithmException, XmlParserException,  ErrorResponseException {
        return minioConnectionFactory.getConnection().getPresignedObjectUrl(GetPresignedObjectUrlArgs.builder().bucket(bucketName).object(objectName).method(Method.GET).build());
    }

    /**
     * Object operations
     */
    public String getObjectURL(String bucketName, String objectName, Integer expires) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException,  ServerException, InternalException, NoSuchAlgorithmException, XmlParserException,  ErrorResponseException {
        return minioConnectionFactory.getConnection().getPresignedObjectUrl(GetPresignedObjectUrlArgs.builder().bucket(bucketName).method(Method.GET).object(objectName).expiry(expires).build());
    }

    public ObjectWriteResponse composeObject(List<ComposeSource> composeSources) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        return minioConnectionFactory.getConnection().composeObject(ComposeObjectArgs.builder().sources(composeSources).build());
    }

    /**
     * save known size object
     *
     * @param bucketName
     * @param objectName
     * @param stream
     * @param contentType
     */
    public ObjectWriteResponse saveKnownSizeObject(String bucketName, String objectName, InputStream stream, long objectSize, String contentType) throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, ErrorResponseException, XmlParserException,  InsufficientDataException, InternalException {
        return saveObject(bucketName, objectName, stream, objectSize, -1, contentType);
    }

    /**
     * save unknown size object
     *
     * @param bucketName
     * @param objectName
     * @param stream
     * @param partSize    object part size, <B>Required</B>
     * @param contentType
     */
    public ObjectWriteResponse saveUnknownSizeObject(String bucketName, String objectName, InputStream stream, long partSize, String contentType) throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, ErrorResponseException, XmlParserException,  InsufficientDataException, InternalException {
        return saveObject(bucketName, objectName, stream, -1, partSize, contentType);
    }

    /**
     * create folder or directory
     *
     * @param bucketName
     * @param objectName folder or directory path relative to bucket root, <B>MUST end with '/'</B>! for example '/path/to/'
     */
    public ObjectWriteResponse mkdir(String bucketName, String objectName) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException, MinioBadRequestException {
        if (!objectName.endsWith("/")) {
            throw new MinioBadRequestException("folder or directory object name MUST end with '/'");
        }
        return saveObject(bucketName, objectName, new ByteArrayInputStream(new byte[]{}), 0, -1, null);
    }

    /**
     * save object
     *
     * @param bucketName
     * @param objectName
     * @param stream
     * @param objectSize
     * @param partSize
     * @param contentType object content type, for example: 'mp4', 'jpg', etc.  can be null if save folder or directory
     */
    public ObjectWriteResponse saveObject(String bucketName, String objectName, InputStream stream, long objectSize, long partSize, String contentType) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        PutObjectArgs putObjectArgs = PutObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .stream(stream, objectSize, partSize)
                .contentType(contentType)
                .build();
        return minioConnectionFactory.getConnection().putObject(putObjectArgs);
    }

    /**
     * save object with extra header and user metadata
     *
     * @param bucketName
     * @param objectName
     * @param stream
     * @param objectSize
     * @param partSize
     * @param headers
     * @param contentType
     * @param userMetadata
     */
    public void saveObjectExtra(String bucketName, String objectName, InputStream stream, long objectSize, long partSize, Map<String, String> headers, String contentType, Map<String, String> userMetadata) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        PutObjectArgs putObjectArgs = PutObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .stream(stream, objectSize, partSize)
                .headers(headers)
                .userMetadata(userMetadata)
                .build();
        minioConnectionFactory.getConnection().putObject(putObjectArgs);
    }

    /**
     * save object with server side Encryption
     *
     * @param bucketName
     * @param objectName
     * @param stream
     * @param objectSize
     * @param partSize
     * @param headers
     * @param contentType
     * @param userMetadata
     * @param serverSideEncryption
     */
    public void saveObjectExtraWithSSE(String bucketName, String objectName, InputStream stream, long objectSize, long partSize, Map<String, String> headers, String contentType, Map<String, String> userMetadata, ServerSideEncryption serverSideEncryption) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        PutObjectArgs putObjectArgs = PutObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .stream(stream, objectSize, partSize)
                .headers(headers)
                .userMetadata(userMetadata)
                .sse(serverSideEncryption)
                .build();
        minioConnectionFactory.getConnection().putObject(putObjectArgs);
    }

    /**
     * get object stat info
     *
     * @param bucketName
     * @param objectName
     * @return
     */
    public StatObjectResponse getObjectInfo(String bucketName, String objectName) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        return minioConnectionFactory.getConnection().statObject(StatObjectArgs.builder().bucket(bucketName).object(objectName).build());
    }

    /**
     * get versioned object stat info
     *
     * @param bucketName
     * @param objectName
     * @return
     */
    public StatObjectResponse getVersionedObjectInfo(String bucketName, String objectName, String versionId) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        return minioConnectionFactory.getConnection().statObject(StatObjectArgs.builder().bucket(bucketName).object(objectName).versionId(versionId).build());
    }

    public void removeObject(String bucketName, String objectName) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        minioConnectionFactory.getConnection().removeObject(RemoveObjectArgs.builder().bucket(bucketName).object(objectName).build());
    }

    public void removeVersionedObject(String bucketName, String objectName, String versionId) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        minioConnectionFactory.getConnection().removeObject(RemoveObjectArgs.builder().bucket(bucketName).object(objectName).versionId(versionId).build());
    }

    /**
     * 批量删除objects
     *
     * @param bucketName
     * @param objectNames
     * @return list of deleting error object name
     */
    public List<String> removeObjects(String bucketName, Collection<String> objectNames) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        List<DeleteObject> objects = objectNames.stream().map(DeleteObject::new).collect(Collectors.toList());
        Iterable<Result<DeleteError>> results = minioConnectionFactory.getConnection().removeObjects(RemoveObjectsArgs.builder().bucket(bucketName).objects(objects).build());
        List<String> errorDeleteObjects = new ArrayList<>();
        for (Result<DeleteError> result : results) {
            errorDeleteObjects.add(result.get().objectName());
            log.error("Error in deleting object {}:{}, code={}, message={}", bucketName, result.get().objectName(), result.get().code(), result.get().message());
        }
        return errorDeleteObjects;
    }

}
