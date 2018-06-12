package com.jianchi;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

import java.io.File;

/**
 * 可以对上传阶段进行跟踪的上传
 */
public class HighLevelTrackMultipartUpload {

    public static void main(String[] args) throws Exception {
        AmazonS3 s3Client = HighLevelMultipartUpload.s3Client;
        TransferManager tm = TransferManagerBuilder.standard()
                .withS3Client(s3Client)
                .build();
        try {
            PutObjectRequest request = new PutObjectRequest(HighLevelMultipartUpload.namespace, HighLevelMultipartUpload.keyName, new
                    File(HighLevelMultipartUpload.filePath));

            // To receive notifications when bytes are transferred, add a
            // ProgressListener to your request.
            request.setGeneralProgressListener(new ProgressListener() {
                public void progressChanged(ProgressEvent progressEvent) {
                    System.out.println("Transferred bytes: " +
                            progressEvent.getBytesTransferred());
                }
            });
            // TransferManager processes all transfers asynchronously,
            // so this call returns immediately.
            Upload upload = tm.upload(request);

            // Optionally, you can wait for the upload to finish before continuing.
            upload.waitForCompletion();
            System.out.println("upload complete");
        }
        catch(AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            HighLevelMultipartUpload.customAbort(tm);
            e.printStackTrace();
        }
        catch(SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            HighLevelMultipartUpload.customAbort(tm);
            e.printStackTrace();
        }
    }
}
