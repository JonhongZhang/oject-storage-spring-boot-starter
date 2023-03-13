package com.junhong.ojectstoragespringbootstarter.connection;

import io.minio.MinioClient;

public class MinioConnectionFactory {
    private final MinioClient.Builder builder;

    public MinioConnectionFactory(MinioClient.Builder builder) {
        this.builder = builder;
    }

//    public waq(MinioClient.Builder builder) {
//        this.builder = builder;
//    }

    public MinioClient getConnection() {
        return builder.build();
    }
}
