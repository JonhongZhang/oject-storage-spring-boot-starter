package com.junhong.ojectstoragespringbootstarter.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Minio server connection settings
 *
 * @author jh
 */
@ConfigurationProperties("storage.minio")
public class MinioProperties {
    /**
     * minio URL, it should be a  URL, domain name, IPv4 address or IPv6 address
     */
    private String url;

    /**
     * minio endpoint, it should be a  URL, domain name, IPv4 address or IPv6 address
     */
    private String endpoint;

    /**
     * minio endpoint, it should be a  URL, domain name, IPv4 address or IPv6 address
     */
    private String bucketName;

    /**
     * uniquely identifies a minio account.
     */
    private String accessKey;
    /**
     * the password to a minio account.
     */
    private String secretKey;

    /**
     * bucket.
     */
    private String bucket;

    /**
     * Not Required, minio server region info， used with cloud minio
     */
    private String region;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    @Override
    public String toString() {
        return "MinioProperties{" +
                "url='" + url + '\'' +
                "endpoint='" + endpoint + '\'' +
                "bucketName='" + bucketName + '\'' +
                ", accessKey='" + accessKey + '\'' +
                ", secretKey='" + secretKey + '\'' +
                ", region='" + region + '\'' +
                '}';
    }
}
