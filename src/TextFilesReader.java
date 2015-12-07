import org.apache.commons.io.FileUtils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by user on 7/8/15.
 */
public class TextFilesReader {
    public static void main(String[] args) throws IOException{

        File[] files = new File(args[0]).listFiles();
        int fileCount= files.length;
//        StringBuilder sb=new StringBuilder();
//        String[] arr= new String[]{"abc","abc2"};
//        sb.append("[\n");

        for(int i=0; i<1250;i=i+50)
            System.out.print(i +" , ");

        ArrayList<List<String>> listOLists = new ArrayList<List<String>>();
        ArrayList<String> latLongObj= new ArrayList<String>();
        ArrayList<String> markerObj = new ArrayList<String>();

        if(fileCount>0)
        {
            File file=files[fileCount-1];
            String name=file.getName();
            if (file.isFile() && !file.getName().substring(0,1).equals(".")) {
                //Path path = FileSystems.getDefault().getPath(args[0] + file.getName());
                List<String> fileLines = FileUtils.readLines(new File(args[0]+"/"+file.getName() ), "UTF-8");
                for(String s: fileLines) {
                   String[] sp= s.split(",");
                    if(sp.length==3) {
                        float lat = Float.parseFloat(sp[0]);
                        float longi = Float.parseFloat(sp[1]);
                        String weight=sp[2];
                        latLongObj.add("{location:new google.maps.LatLng(" + lat + "," + longi + "),weight:"+weight+"}");
                       // markerObj.add("new google.maps.Marker(" + Float.parseFloat(sp[0]) + "," + Float.parseFloat(sp[1]) + ")");

                    }

                }
                listOLists.add(latLongObj);
               // listOLists.add(markerObj);
                System.out.println("The latlong obj is"+listOLists.get(0));
                System.out.println("The marker obj is"+listOLists.get(1));

//
//                int count=0;
//                for(String s: fileLines) {
//                    sb.append("  new google.maps.LatLng(");
//                    sb.append(s + ")");
//                    if(count++ != fileLines.size()-1)
//                        sb.append(",");
//                }
//                sb.append("]");
                System.out.println("List Received");

            }
        }





    }
}
