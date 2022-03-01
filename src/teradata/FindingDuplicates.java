/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package teradata;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.io.FileUtils;

/**
 *
 * @author cKakarla
 */
public class FindingDuplicates {

    public static void main(String[] args) {
        Set<String> setOfNames = new HashSet<>();
        //FileUtils.readFileToString(new File("C:\\Work Area\\dataqueries.txt"), "");
        try {
            String fileContent = FileUtils.readFileToString(new File("C:\\Users\\cKakarla\\OneDrive - Quest\\Desktop\\Sprint9.txt"));
            String arrayofNames[] = fileContent.split("\n");
            for (int i = 0; i < arrayofNames.length; i++) {
                setOfNames.add(arrayofNames[i]);

            }
            System.out.println("setOfNames===========" + setOfNames.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
