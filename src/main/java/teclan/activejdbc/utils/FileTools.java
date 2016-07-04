package teclan.activejdbc.utils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileTools {
    private static final Logger LOGGER = LoggerFactory
            .getLogger(FileTools.class);

    /**
     * @author Teclan
     * 
     *         获取文件编码
     * @param filePath
     * @return
     * @throws Exception
     */
    public static String getCoding(String filePath) throws Exception {

        File file = new File(filePath);

        if (!file.isFile() || !file.exists()) {
            return "unkonw";
        }

        BufferedInputStream bis = new BufferedInputStream(
                new FileInputStream(filePath));
        int p = (bis.read() << 8) + bis.read();
        String code = null;

        switch (p) {
        case 0xefbb:
            code = "UTF-8";
            break;
        case 0xfffe:
            code = "Unicode";
            break;
        case 0xfeff:
            code = "UTF-16BE";
            break;
        default:
            code = "GBK";
        }
        return code;
    }

    /**
     * @author Teclan
     * 
     *         读取文本文件内容
     * @param file
     * @return 文本内容(字符串形式)
     */
    public static String getContent(File file) {
        StringBuilder content = new StringBuilder();
        try {
            String encoding = FileTools.getCoding(file.getAbsolutePath())
                    .toUpperCase();

            if (encoding.contains("GBK")) {
                encoding = "GBK";
            } else {
                encoding = "UTF-8";
            }

            if (file.isFile() && file.exists()) { // 判断文件是否存在
                InputStreamReader read = new InputStreamReader(
                        new FileInputStream(file), encoding);// 考虑到编码格式
                BufferedReader bufferedReader = new BufferedReader(read);
                String line = null;
                while ((line = bufferedReader.readLine()) != null) {
                    content.append("\n").append(line);
                }
                read.close();
            } else {
                LOGGER.error("找不到指定的文件:{}", file.getAbsolutePath());
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return null;
        }

        return content.toString();
    }

}
