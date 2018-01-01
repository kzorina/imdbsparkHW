package imdb_spark; /**
 * Created by zorka_000 on 04.12.2017.
 */
import java.io.*;
import java.util.zip.GZIPInputStream;
import lombok.Setter;
import org.apache.commons.io.FileUtils;
import java.net.URL;

/**
 * Class for downloading files from the 'current' folder in the
 * imdb-datasets.
 */

public class GetObject {

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
            //System.out.println("Done with untaring");
        } catch (IOException ex) {
            ex.printStackTrace();
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
     gunzipIt("./data/".concat(file_name.concat(".tsv.gz")),"./data/".concat(file_name.concat(".csv")));
    }

}