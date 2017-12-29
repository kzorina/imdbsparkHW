package imdb_spark; /**
 * Created by zorka_000 on 04.12.2017.
 */
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import lombok.Setter;
import java.util.HashMap;
import org.apache.commons.io.FileUtils;
import java.net.URL;

/**
 * Class for downloading files from the 'current' folder in the
 * imdb-datasets s3 bucket.
 *
 * Use with AWS Java SDK 1.11.156 or later.
 */

public class GetObject {
    private static String bucketName = "imdb-datasets";
    private static String key = "documents/v1/current/";
    private static String pathToProperties = "./data/AWSCredentials.properties";
    private static String userName = "kzorina";
    @Setter
    private static String file_name = "title.principals";
    public static void gunzipIt(String input_file, String output_file) {
        byte[] buffer = new byte[1024];
        try {
            GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(input_file));
            FileOutputStream out = new FileOutputStream(output_file);
            int len;
            while ((len = gzis.read(buffer)) > 0) {
                out.write(buffer, 0, len);
            }
            gzis.close();
            out.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public static void downloadFile_aws(String file_name) throws IOException, InterruptedException {
        setFile_name(file_name);
        ProfileCredentialsProvider credentialsProvider =
                new ProfileCredentialsProvider(pathToProperties,
                        userName);
        AmazonS3 s3Client = new AmazonS3Client(credentialsProvider);
        try {
            // Note: It's necessary to set RequesterPays to true
            key = key.concat(file_name).concat(".tsv.gz");
            System.out.println(bucketName);
            System.out.println(key);
            GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, key)
                    .withRequesterPays(true);

            System.out.println("Downloading object");
            S3Object s3object = s3Client.getObject(getObjectRequest);
            System.out.println("Content-Type: " +
                    s3object.getObjectMetadata().getContentType());
            writeFile(s3object.getObjectContent());
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which" +
                    " means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means" +
                    " the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }

 public static void downloadFile(String file_name) throws IOException, InterruptedException {
        setFile_name(file_name);
		File file_gz = new File("./data/".concat(file_name.concat(".tsv.gz")));
        file_gz.createNewFile();
		URL url = new URL(Const.FILES_URLS.get(file_name));
        FileUtils.copyURLToFile(url, file_gz);
		File file = new File("./data/".concat(file_name.concat(".csv")));
        file.createNewFile();
        gunzipIt(file_name.concat(".tsv.gz"),"./data/".concat(file_name.concat(".csv")));
    }
    private static void writeFile(InputStream input) throws IOException, InterruptedException {
        byte[] buf = new byte[1024 * 1024];
        OutputStream out = new FileOutputStream(file_name.concat(".csv.gz"));
        int count;
        while ((count = input.read(buf)) != -1) {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            out.write(buf, 0, count);
        }
        out.close();
        input.close();
        File file = new File("./data/".concat(file_name.concat(".csv")));
        file.createNewFile();
        gunzipIt(file_name.concat(".csv.gz"),"./data/".concat(file_name.concat(".csv")));
    }
}
//}
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.OutputStream;
//import java.io.FileOutputStream;
//
//import com.amazonaws.AmazonClientException;
//import com.amazonaws.AmazonServiceException;
//import com.amazonaws.auth.profile.ProfileCredentialsProvider;
//import com.amazonaws.services.s3.AmazonS3;
//import com.amazonaws.services.s3.AmazonS3Client;
//import com.amazonaws.services.s3.model.GetObjectRequest;
//import com.amazonaws.services.s3.model.S3Object;
//
///**
// * Sample class for downloading name.basics.csv.gz from the 'current' folder in the
// * imdb-datasets s3 bucket.
// *
// * Use with AWS Java SDK 1.11.156 or later.
// */
//
//public class old_shit.GetObject {
//    private static String bucketName = "imdb-datasets";
//    private static String key        = "documents/v1/current/title.basics.csv.gz";
//
//    public static void main(String[] args) throws IOException, InterruptedException {
//        ProfileCredentialsProvider credentialsProvider =
//                new ProfileCredentialsProvider("Z:/Studing/UCU/Programming/aws/AWSCredentials.properties",
//                        "zorina");
//
//        AmazonS3 s3Client = new AmazonS3Client(credentialsProvider);
//
//        try {
//            // Note: It's necessary to set RequesterPays to true
//            GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, key)
//                    .withRequesterPays(true);
//
//            System.out.println("Downloading object");
//
//            S3Object s3object = s3Client.getObject(getObjectRequest);
//
//            System.out.println("Content-Type: "  +
//                    s3object.getObjectMetadata().getContentType());
//
//            writeFile(s3object.getObjectContent());
//        } catch (AmazonServiceException ase) {
//            System.out.println("Caught an AmazonServiceException, which" +
//                    " means your request made it " +
//                    "to Amazon S3, but was rejected with an error response" +
//                    " for some reason.");
//            System.out.println("Error Message:    " + ase.getMessage());
//            System.out.println("HTTP Status Code: " + ase.getStatusCode());
//            System.out.println("AWS Error Code:   " + ase.getErrorCode());
//            System.out.println("Error Type:       " + ase.getErrorType());
//            System.out.println("Request ID:       " + ase.getRequestId());
//        } catch (AmazonClientException ace) {
//            System.out.println("Caught an AmazonClientException, which means"+
//                    " the client encountered " +
//                    "an internal error while trying to " +
//                    "communicate with S3, " +
//                    "such as not being able to access the network.");
//            System.out.println("Error Message: " + ace.getMessage());
//        }
//    }
//
//    private static void writeFile(InputStream input) throws IOException, InterruptedException {
//        byte[] buf = new byte[1024 * 1024];
//        OutputStream out = new FileOutputStream("name.basics.csv.gz");
//        int count;
//        while ((count = input.read(buf)) != -1) {
//            if (Thread.interrupted()) {
//                throw new InterruptedException();
//            }
//            out.write(buf, 0, count);
//        }
//        out.close();
//        input.close();
//    }
//}
