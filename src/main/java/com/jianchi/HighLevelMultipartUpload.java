package com.jianchi;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

import java.io.File;
import java.util.Date;

/**
 * 封装断点上传逻辑代码
 */
public class HighLevelMultipartUpload {

    public static final String ACCESS_KEY = "LIETZEPH97B3ECFF8G3S";
    public static final String PASSWORD = "k9rRP84WLcDTMyNSstpJiNPFvBOKnMDjXQdRZdsF";
    public static final  String url = "http://172.16.48.188:7480";
    public static final  String namespace = "test";
    public static final  String keyName = "asdfa";
    public static final  String filePath = "C:\\Users\\fulushou\\Downloads\\cmake-3.11.1-win64-x64.msi";

    public static final AmazonS3 s3Client = buildAmazonS3Conn();

    public static void main(String[] args) throws Exception {
        TransferManager tm = TransferManagerBuilder.standard()
                .withS3Client(s3Client)
                .build();
        try {
            // TransferManager processes all transfers asynchronously,
            // so this call returns immediately.
            Upload upload = tm.upload(namespace, keyName, new File(filePath));
            System.out.println("Object upload started");

            // Optionally, wait for the upload to finish before continuing.
            upload.waitForCompletion();
            System.out.println("Object upload complete");
        }
        catch(AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            customAbort(tm);

            e.printStackTrace();
        }
        catch(SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            customAbort(tm);
            e.printStackTrace();
        }
    }

    // 自动取消多久之前没有上传成功的。默认取消8小时之前没有上传成功的。
    public static void customAbort(TransferManager tm ){
        long sevenDays = 1000 * 60 * 60 * 8 ;
        Date oneWeekAgo = new Date(System.currentTimeMillis() - sevenDays);
        tm.abortMultipartUploads(namespace, oneWeekAgo);
    }

    public static ClientConfiguration getClientConfiguration() {
        ClientConfiguration clientConfiguration = new ClientConfiguration();

        clientConfiguration.setConnectionTimeout(10_000);
        clientConfiguration.setMaxConnections(100);
        clientConfiguration.setMaxErrorRetry(3);
        clientConfiguration.setProtocol(Protocol.HTTP);
        clientConfiguration.setSocketTimeout(60_000);
        clientConfiguration.setUseTcpKeepAlive(true);
        clientConfiguration.setSocketBufferSizeHints(65536, 65536);
        clientConfiguration.withSignerOverride("S3SignerType");
        return clientConfiguration;
    }

    public static AmazonS3 buildAmazonS3Conn() {
        AWSCredentials credentials = new BasicAWSCredentials(ACCESS_KEY, PASSWORD);
        AWSStaticCredentialsProvider awsStaticCredentialsProvider = new AWSStaticCredentialsProvider(credentials);
        AmazonS3  conn = new AmazonS3Client(awsStaticCredentialsProvider,getClientConfiguration());
//		AmazonS3 conn = new AmazonS3Client(credentials);
        conn.setEndpoint(url);
        conn.setS3ClientOptions(S3ClientOptions.builder().setPathStyleAccess(true).build());
        return conn;
    }
}
