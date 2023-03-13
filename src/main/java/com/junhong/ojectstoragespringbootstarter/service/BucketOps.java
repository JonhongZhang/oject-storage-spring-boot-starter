package com.junhong.ojectstoragespringbootstarter.service;

import com.junhong.ojectstoragespringbootstarter.connection.MinioConnectionFactory;
import io.minio.BucketExistsArgs;
import io.minio.CloseableIterator;
import io.minio.DeleteBucketEncryptionArgs;
import io.minio.DeleteBucketLifecycleArgs;
import io.minio.DeleteBucketNotificationArgs;
import io.minio.DeleteBucketPolicyArgs;
import io.minio.DeleteBucketTagsArgs;
import io.minio.GetBucketEncryptionArgs;
import io.minio.GetBucketLifecycleArgs;
import io.minio.GetBucketNotificationArgs;
import io.minio.GetBucketPolicyArgs;
import io.minio.GetBucketTagsArgs;
import io.minio.ListenBucketNotificationArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.RemoveBucketArgs;
import io.minio.Result;
import io.minio.ServerSideEncryption;
import io.minio.SetBucketEncryptionArgs;
import io.minio.SetBucketLifecycleArgs;
import io.minio.SetBucketNotificationArgs;
import io.minio.SetBucketPolicyArgs;
import io.minio.SetBucketTagsArgs;
import io.minio.errors.BucketPolicyTooLargeException;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import io.minio.messages.Bucket;
import io.minio.messages.EventType;
import io.minio.messages.LifecycleConfiguration;
import io.minio.messages.NotificationConfiguration;
import io.minio.messages.NotificationRecords;
import io.minio.messages.ObjectLockConfiguration;
import io.minio.messages.QueueConfiguration;
import io.minio.messages.RetentionDuration;
import io.minio.messages.RetentionMode;
import io.minio.messages.SseConfiguration;
import io.minio.messages.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Bucket operator
 *
 * @author jh
 */
public class BucketOps {
    private Logger log = LoggerFactory.getLogger(BucketOps.class);

    private final MinioConnectionFactory minioConnectionFactory;

    private String bucket;
    private ServerSideEncryption serverSideEncryption;
    private String region;

    private Map<String, String> extraHeaders;

    private Map<String, String> extraQueryParams;

    public BucketOps(MinioConnectionFactory minioConnectionFactory) {
        this.minioConnectionFactory = minioConnectionFactory;
    }

    private BucketOps bucket(String bucketName) {
        this.bucket = bucketName;
        return this;
    }

    /**
     * Set bucket server side encryption to operate
     *
     * @param serverSideEncryption
     * @return
     */
    public BucketOps sse(ServerSideEncryption serverSideEncryption) {
        this.serverSideEncryption = serverSideEncryption;
        return this;
    }

    /**
     * Set bucket region to operate
     *
     * @param region
     * @return
     */
    public BucketOps region(String region) {
        this.region = region;
        return this;
    }

    public BucketOps extraHeaders(Map<String, String> extraHeaders){
        this.extraHeaders = extraHeaders;
        return this;
    }

    public BucketOps extraQueryParams(Map<String, String> extraQueryParams){
        this.extraQueryParams = extraQueryParams;
        return this;
    }

    private MinioClient connection() {
        return minioConnectionFactory.getConnection();
    }

    /**
     * check bucket exists
     */
    public boolean bucketExists() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        return connection().bucketExists(BucketExistsArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Deletes encryption configuration of a bucket.
     */
    public void deleteBucketEncryption() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        connection().deleteBucketEncryption(DeleteBucketEncryptionArgs.builder()
                .bucket(this.bucket)
                .region(region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Deletes life-cycle configuration of a bucket.
     */
    public void deleteBucketLifeCycle() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
//        connection().getBucketLifeCycle(GetBucketLifeCycleArgs.builder()
        connection().deleteBucketLifecycle(DeleteBucketLifecycleArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Deletes notification configuration of a bucket.
     */
    public void deleteBucketNotification() throws IOException, InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, ServerException, ErrorResponseException, XmlParserException , InsufficientDataException, InternalException {
        connection().deleteBucketNotification(DeleteBucketNotificationArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Deletes bucket policy configuration of a bucket.
     */
    public void deleteBucketPolicy() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        connection().deleteBucketPolicy(DeleteBucketPolicyArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Deletes tags of a bucket.
     */
    public void deleteBucketTags() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        connection().deleteBucketTags(DeleteBucketTagsArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

//    /**
//     * Deletes default object retention in a bucket.
//     */
//    public void deleteDefaultRetention() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
//        connection().deleteDefaultRetention(DeleteDefaultRetentionArgs.builder()
//                .bucket(this.bucket)
//                .region(this.region)
//                .extraHeaders(this.extraHeaders)
//                .extraQueryParams(this.extraQueryParams)
//                .build());
//    }
//
//    /**
//     * Disables object versioning feature in a bucket.
//     * 禁用存储桶中的对象版本控制功能。
//     */
//    public void disableVersioning() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
//        connection().disableVersioning(DisableVersioningArgs.builder()
//                .bucket(this.bucket)
//                .region(this.region)
//                .extraHeaders(this.extraHeaders)
//                .extraQueryParams(this.extraQueryParams)
//                .build());
//    }

//    /**
//     * Enables object versioning feature in a bucket.
//     * 启用存储桶中的对象版本控制功能。
//     */
//    public void enableVersioning() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
//        connection().enableVersioning(EnableVersioningArgs.builder()
//                .bucket(this.bucket)
//                .region(this.region)
//                .extraHeaders(this.extraHeaders)
//                .extraQueryParams(this.extraQueryParams)
//                .build());
//    }

    /**
     * Gets encryption configuration of a bucket.
     */
    public SseConfiguration getBucketEncryption() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        return connection().getBucketEncryption(GetBucketEncryptionArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Gets life-cycle configuration of a bucket.
     */
    public String getBucketLifeCycle() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
//        return connection().getBucketLifeCiycle(GetBucketLifeCycleArgs.builder()
        return connection().getBucketLifecycle(GetBucketLifecycleArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build()).toString();
    }

    /**
     * Gets notification configuration of a bucket.
     */
    public NotificationConfiguration getBucketNotification() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        return connection().getBucketNotification(GetBucketNotificationArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Gets bucket policy configuration of a bucket.
     */
    public String getBucketPolicy() throws IOException, InvalidKeyException, InvalidResponseException, BucketPolicyTooLargeException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  InsufficientDataException, ErrorResponseException {
        return connection().getBucketPolicy(GetBucketPolicyArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Gets tags of a bucket.
     */
    public Tags getBucketTags() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        return connection().getBucketTags(GetBucketTagsArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

//    /**
//     * Gets default object retention in a bucket.
//     * 获取存储空间中的默认对象保留。
//     */
//    public ObjectLockConfiguration getDefaultRetention() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
//        return connection().getDefaultRetention(GetDefaultRetentionArgs.builder()
//                .bucket(this.bucket)
//                .region(this.region)
//                .extraHeaders(this.extraHeaders)
//                .extraQueryParams(this.extraQueryParams)
//                .build());
//    }
//
//    public boolean isVersioningEnabled() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
//        return connection().isVersioningEnabled(IsVersioningEnabledArgs.builder()
//                .bucket(this.bucket)
//                .region(this.region)
//                .extraHeaders(this.extraHeaders)
//                .extraQueryParams(this.extraQueryParams)
//                .build());
//    }

    /**
     * Lists bucket information of all buckets.
     *
     * @return
     */
    public List<Bucket> listBuckets() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        return connection().listBuckets();
    }

    /**
     * Listens events of object prefix and suffix of a bucket.
     * <p>
     * The returned closable iterator is lazily evaluated
     * hence its required to iterate to get new records
     * and must be used with try-with-resource to release
     * underneath network resources.
     *
     * @return
     */
    public CloseableIterator<Result<NotificationRecords>> listenBucketNotification(String objectPrefix, String objectSuffix, String... events) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        if (objectPrefix == null) {
            objectPrefix = "";
        }
        if (objectSuffix == null) {
            objectSuffix = "";
        }
        return connection().listenBucketNotification(ListenBucketNotificationArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .prefix(objectPrefix)
                .suffix(objectSuffix)
                .events(events)
                .build());
    }
//
//    /**
//     * Lists incomplete object upload information of a bucket.
//     * 列出bucket不完整的文件上传信息。
//     */
//    public void listIncompleteUploads() {
//        connection().listIncompleteUploads(ListIncompleteUploadsArgs.builder()
//                .bucket(this.bucket)
//                .region(this.region)
//                .extraHeaders(this.extraHeaders)
//                .extraQueryParams(this.extraQueryParams)
//                .build());
//    }

//    /**
//     * Lists incomplete object upload information of a bucket recursively.
//     * 递归列出bucket的不完整上传信息。
//     */
//    public void listIncompleteUploads(boolean recursive) {
//        connection().listIncompleteUploads(ListIncompleteUploadsArgs.builder()
//                .bucket(this.bucket)
//                .region(this.region)
//                .extraHeaders(this.extraHeaders)
//                .recursive(recursive)
//                .extraQueryParams(this.extraQueryParams)
//                .build());
//    }

//    /**
//     * Lists incomplete object upload information of a bucket.
//     */
//    public void listIncompleteUploadsWithPrefix(String prefix) {
//        connection().listIncompleteUploads(ListIncompleteUploadsArgs.builder()
//                .bucket(this.bucket)
//                .region(this.region)
//                .extraHeaders(this.extraHeaders)
//                .extraQueryParams(this.extraQueryParams)
//                .prefix(prefix)
//                .build());
//    }
    //TODO: uploadIdMarker and maxUpload

    /**
     * Creates a bucket with given region and object lock feature enabled.
     */
    public void makeBucket() throws IOException, InvalidKeyException, InvalidResponseException, InvalidKeyException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        connection().makeBucket(MakeBucketArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Creates a object-lock enabled bucket with given region and object lock feature enabled.
     */
    public void makeObjectLockEnabledBucket() throws IOException, InvalidKeyException, InvalidResponseException, InvalidKeyException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        connection().makeBucket(MakeBucketArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .objectLock(true)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

    /**
     * Removes an empty bucket.
     */
    public void removeBucket() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        connection().removeBucket(RemoveBucketArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .build());
    }

//    /**
//     * Removes incomplete uploads of an object.
//     * 删除对象的不完整上传。
//     */
//    public void removeIncompleteUpload(String objectName) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
//        connection().removeIncompleteUpload(RemoveIncompleteUploadArgs.builder()
//                .bucket(this.bucket)
//                .region(this.region)
//                .extraHeaders(this.extraHeaders)
//                .object(objectName)
//                .extraQueryParams(this.extraQueryParams)
//                .build());
//    }

//    /**
//     * Removes incomplete uploads of an object.
//     */
//    public void removeIncompleteVersionedUpload(String objectName, String versionId) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
//        connection().removeIncompleteUpload(RemoveIncompleteUploadArgs.builder()
//        connection().removeIncompleteUpload(RemoveIncompleteUploadArgs.builder()
//                .bucket(this.bucket)
//                .region(this.region)
//                .extraHeaders(this.extraHeaders)
//                .object(objectName)
//                .versionId(versionId)
//                .extraQueryParams(this.extraQueryParams)
//                .build());
//    }

    /**
     * Sets encryption configuration of a bucket.
     */
    public void setBucketEncryption(SseConfiguration sseConfiguration) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        connection().setBucketEncryption(SetBucketEncryptionArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .config(sseConfiguration)
                .build());
    }

    /**
     * Sets life-cycle configuration to a bucket.
     * <p>
     * for example:
     * <LifecycleConfiguration>
     * <Rule>
     * <ID>expire-bucket</ID>
     * <Prefix></Prefix>
     * <Status>Enabled</Status>
     * <Expiration>
     * <Days>365</Days>
     * </Expiration>
     * </Rule>
     * </LifecycleConfiguration>
     */
//    public void setBucketLifeCycle(String lifeCycleXml) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
//        connection().setBucketLifecycle(SetBucketLifecycleArgs.builder()
//                .bucket(this.bucket)
//                .region(this.region)
//                .extraHeaders(this.extraHeaders)
//                .extraQueryParams(this.extraQueryParams)
//                .config(lifeCycleXml)
//                .build());
//    }
//    
    public void setBucketLifeCycle(LifecycleConfiguration config) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        connection().setBucketLifecycle(SetBucketLifecycleArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .config(config)
                .build());
    }

    /**
     * Sets notification configuration to a bucket.
     */
    public void setBucketNotification(NotificationConfiguration notification) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        connection().setBucketNotification(SetBucketNotificationArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .config(notification)
                .build());
    }

    /**
     * Sets notification configuration to a bucket.
     */
    public void setBucketNotification(List<EventType> eventTypes, String queue, String prefixRule, String suffixRule) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        QueueConfiguration queueConfiguration = new QueueConfiguration();
        queueConfiguration.setQueue(queue);
        queueConfiguration.setEvents(eventTypes);
        queueConfiguration.setPrefixRule(prefixRule);
        queueConfiguration.setSuffixRule(suffixRule);

        NotificationConfiguration notificationConfiguration = new NotificationConfiguration();
        notificationConfiguration.setQueueConfigurationList(Arrays.asList(queueConfiguration));
        setBucketNotification(notificationConfiguration);
        setBucketNotification(notificationConfiguration);
    }

    /**
     * set bucket policy, for exmaple
     * <p>
     * {
     * "Statement": [
     * {
     * "Action": [
     * "s3:GetBucketLocation",
     * "s3:ListBucket"
     * ],
     * "Effect": "Allow",
     * "Principal": "*",
     * "Resource": "arn:aws:s3:::my-bucketname"
     * },
     * {
     * "Action": "s3:GetObject",
     * "Effect": "Allow",
     * "Principal": "*",
     * "Resource": "arn:aws:s3:::my-bucketname/myobject*"
     * }
     * ],
     * "Version": "2012-10-17"
     * }
     */
    public void setBucketPolicy(String policyConfig) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        connection().setBucketPolicy(SetBucketPolicyArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .config(policyConfig)
                .build());
    }

    /**
     * Sets tags to a bucket.
     */
    public void setBucketTags(Map<String, String> tags) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
        connection().setBucketTags(SetBucketTagsArgs.builder()
                .bucket(this.bucket)
                .region(this.region)
                .extraHeaders(this.extraHeaders)
                .extraQueryParams(this.extraQueryParams)
                .tags(tags)
                .build());
    }

//    /**
//     * Sets default object retention in a bucket.
//     * 设置存储空间中的默认对象保留
//     */
//    public void setDefaultRetention(ObjectLockConfiguration objectLockConfiguration) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
//        connection().setDefaultRetention(SetDefaultRetentionArgs.builder()
//                .bucket(this.bucket)
//                .region(this.region)
//                .extraHeaders(this.extraHeaders)
//                .extraQueryParams(this.extraQueryParams)
//                .config(objectLockConfiguration)
//                .build());
//    }

//    /**
//     * Sets default object retention in a bucket.
//     * 设置存储空间中的默认对象保留。
//     */
//    public void setDefaultRetention(RetentionMode retentionMode, RetentionDuration retentionDuration) throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException,  ErrorResponseException {
//        ObjectLockConfiguration objectLockConfiguration = new ObjectLockConfiguration(retentionMode, retentionDuration);
//        connection().setDefaultRetention(SetDefaultRetentionArgs.builder()
//                .bucket(this.bucket)
//                .region(this.region)
//                .extraHeaders(this.extraHeaders)
//                .extraQueryParams(this.extraQueryParams)
//                .config(objectLockConfiguration)
//                .build());
//    }
}
