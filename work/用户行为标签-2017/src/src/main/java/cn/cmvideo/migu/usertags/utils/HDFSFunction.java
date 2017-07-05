package cn.cmvideo.migu.usertags.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.LinkedHashSet;
import java.util.Set;

public class HDFSFunction {
    private static Logger log = LoggerFactory.getLogger(HDFSFunction.class);

    private static long minFileSizeThreshold = 32 * 1024 * 1024L;   //32MB以下的文件视为小文件
    private static Configuration conf = new Configuration();
    private static FileSystem hdfs = null;

    public static FileSystem getFileSystem() {
        if (null == hdfs) {
            synchronized (HDFSFunction.class) {
                if (null == hdfs) {
                    try {
                        hdfs = FileSystem.get(conf);
                        log.info("hdfs filesystem opened");
                    } catch (IOException e) {
                        log.warn(e.getMessage());
                    }
                }
            }
        }
        return hdfs;
    }

    public static void closeFileSystem() {
        if (hdfs != null) {
            try {
                hdfs.close();
                log.info("hdfs filesystem closed");
            } catch (IOException e) {
                log.warn(e.getMessage());
            }
        }
        hdfs = null;
    }

    public static boolean isExists(Path filePath) {
        try {
            FileSystem hdfs = getFileSystem();
            return hdfs.exists(filePath);
        } catch (Exception e) {
            log.warn(e.getMessage());
        }
        return false;
    }

    public static long getLength(Path filePath) {
        if (isExists(filePath)) {
            try {
                return getFileSystem().getContentSummary(filePath).getLength();
            } catch (Exception e) {
                log.warn(e.getMessage());
            }
        }
        return 0;
    }

    public static boolean isFile(Path filePath) {
        try {
            return getFileSystem().isFile(filePath);
        } catch (Exception e) {
            log.warn(e.getMessage());
        }
        return false;
    }

    public static boolean isDirectory(Path filePath) {
        if (isExists(filePath)) {
            try {
                return getFileSystem().isDirectory(filePath);
            } catch (Exception e) {
                log.warn(e.getMessage());
            }
        }
        return false;
    }

    public static Set<Path> getChildPath(Path filePath) {
        Set<Path> paths = new LinkedHashSet<Path>();
        try {
            if (isExists(filePath) && isDirectory(filePath)) {
                FileStatus fileList[] = getFileSystem().listStatus(filePath);
                for (FileStatus status : fileList) {
                    paths.add(status.getPath());
                }
            }
        } catch (Exception e) {
            log.warn(e.getMessage());
        }
        return paths;
    }

    public static Set<Path> getCheckPath(Set<String> datetimes, Set<Path> partitionList) {
        Set<Path> paths = new LinkedHashSet<Path>();
        for (Path path : partitionList) {
            for (String date : datetimes) {
                if (path.toString().contains(date)) {
                    paths.add(path);
                }
            }
        }
        return paths;
    }

    public static boolean isPartitionExist(String datetime, Set<Path> partitionList) {
        for (Path path : partitionList) {
            if (path.toString().contains(datetime)) {
                return true;
            }
        }
        return false;
    }

    public static Path getPartitionPath(String datetime, Set<Path> partitionList) {
        for (Path path : partitionList) {
            if (path.toString().contains(datetime)) {
                return path;
            }
        }
        return null;
    }

    public static int getSmallSizeFileCount(Path filePath) {
        Set<Path> files = getFiles(filePath);
        int count = 0;
        for (Path path : files) {
            if (getLength(path) < minFileSizeThreshold) {
                count++;
            }
        }
        return count;
    }

    public static int getTotalFileCount(Path filePath) {
        return getFiles(filePath).size();
    }

    public static long getModificationTime(Path filePath) {
        if (isExists(filePath)) {
            try {
                return getFileSystem().getFileStatus(filePath).getModificationTime();
            } catch (Exception e) {
                log.warn(e.getMessage());
            }
        }
        return 0L;
    }

    public static FileStatus getStatus(Path filePath) {
        if (isExists(filePath)) {
            try {
                return getFileSystem().getFileStatus(filePath);
            } catch (Exception e) {
                log.warn(e.getMessage());
            }
        }
        return null;
    }

    public static Set<Path> getFiles(Path folderPath) {
        Set<Path> paths = new LinkedHashSet<Path>();
        try {
            if (folderPath != null && getFileSystem().exists(folderPath)) {
                FileStatus[] fileStatus = getFileSystem().listStatus(folderPath);
                for (FileStatus fileStatu : fileStatus) {
                    if (fileStatu.isFile()) {
                        Path oneFilePath = fileStatu.getPath();
                        if ((!oneFilePath.getName().endsWith(".tmp")) && (!oneFilePath.getName().endsWith("._COPYING_"))) {   //濡傛灉鏄痶mp鏂囦欢鍒欎唬琛ㄦ枃浠惰繕鍦ㄥ啓鍏ワ紝鎺掗櫎
                            paths.add(oneFilePath);
                        }
                        paths.add(oneFilePath);
                    } else {
                        paths.addAll(getFiles(fileStatu.getPath()));
                    }
                }
            }
        } catch (Exception e) {
            log.warn(e.getMessage());
        }
        return paths;
    }

    public static String toMegaByte(long bytesCount) {
        DecimalFormat df = new DecimalFormat("0.000");
        return df.format((double) bytesCount / 1024 / 1024) + "MB";
    }

    public static String toGigaByte(long bytesCount) {
        DecimalFormat df = new DecimalFormat("0.000");
        return df.format((double) bytesCount / 1024 / 1024 / 1024) + "GB";
    }

    public static void main(String[] args) {
        Path path = new Path("/user/hadoop/ods/kesheng/20161215/16/kesheng.1481788800037.gz.tmp");
        String fileName = "kesheng.1481788800037.gz.tmp";
        if (fileName.endsWith(".tmp") || fileName.endsWith("._COPYING_")) {
            System.out.println("yezr");
        }
    }
}
