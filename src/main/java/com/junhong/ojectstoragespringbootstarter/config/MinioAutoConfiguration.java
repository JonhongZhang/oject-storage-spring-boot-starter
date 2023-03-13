package com.junhong.ojectstoragespringbootstarter.config;

import com.junhong.ojectstoragespringbootstarter.connection.MinioConnectionFactory;
import com.junhong.ojectstoragespringbootstarter.service.MinioTemplate;
import io.minio.MinioClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;


@EnableConfigurationProperties(MinioProperties.class)
@ConditionalOnProperty(prefix = "storage", name = "type", havingValue = "minio")
public class MinioAutoConfiguration {
    @Autowired
    MinioProperties minioProperties;

    /**
     * generate Minio Client
     *
     * @return Minio Client bean
     */
    @Bean
    @ConditionalOnClass(MinioClient.class)
    public MinioConnectionFactory minioConnectionFactory() {
        MinioClient.Builder builder = MinioClient.builder();
        if (!StringUtils.hasText(minioProperties.getRegion())) {
            builder.region(minioProperties.getRegion());
        }
        builder
                .endpoint(minioProperties.getUrl())
                .credentials(minioProperties.getAccessKey(), minioProperties.getSecretKey());
        return new MinioConnectionFactory(builder);
    }

    @Bean
    @ConditionalOnBean(MinioConnectionFactory.class)
    public MinioTemplate minioTemplate(MinioConnectionFactory minioConnectionFactory) {
        return new MinioTemplate(minioConnectionFactory);
    }
}
