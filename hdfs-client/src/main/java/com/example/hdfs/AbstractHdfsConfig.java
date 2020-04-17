package com.example.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @className: HdfsConsts.java
 * @package com.example.hdfs
 * @description:
 * @date 2020/4/15 15:49
 */
public abstract class AbstractHdfsConfig {

    protected static final String NAME_NODE_PATH = "hdfs://192.168.1.102:9000";
    protected static final String LIUNX_USER = "hadoop";

    protected static Configuration configuration = new Configuration();
    protected static FileSystem fs = initFileSystem();

    /**
     * 初始化文件系统配置
     */
    private static FileSystem initFileSystem() {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI(AbstractHdfsConfig.NAME_NODE_PATH),configuration, AbstractHdfsConfig.LIUNX_USER);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return fs;
    }

}
