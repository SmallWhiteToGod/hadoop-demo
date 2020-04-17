package com.example.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @className: HDFSClient.java
 * @package com.example.hdfs
 * @description:
 * @date 2020/4/15 15:36
 */
public class HDFSClient extends AbstractHdfsConfig{

    /**
     * 上传
     */
    @Test
    public void upload() throws Exception {
        configuration.set("dfs.replication","1");  //设置副本数量 参数优先级>本地配置>服务器配置
        fs = FileSystem.get(new URI(NAME_NODE_PATH),configuration,LIUNX_USER);
        fs.copyFromLocalFile(new Path("C:\\Users\\ALIENWARE\\Desktop\\ccc.txt"), new Path("/test/0415/ccc.txt"));

        fs.close();
    }


    /**
     * 下载
     */
    @Test
    public void downLoad() throws IOException {
        //是否删除原文件,hdfs地址,本地地址,是否开启本地校验
        fs.copyToLocalFile(false,new Path("/test/0415/ccc.txt"),
                new Path("C:\\Users\\ALIENWARE\\Desktop\\ccc.txt"),true);

        fs.close();
    }

    /**
     * 删除
     */
    @Test
    public void delete() throws IOException, InterruptedException {
        fs.mkdirs(new Path("/test/0416/111.txt"));
        fs.mkdirs(new Path("/test/0416/222.txt"));
        //false 删除文件  true 删除文件夹

        Thread.sleep(5000);
        fs.delete(new Path("/test/0416/222.txt"),false);
        fs.delete(new Path("/test/0416"),true);

        fs.close();
    }

    /**
     * 修改文件名
     */
    @Test
    public void rename() throws IOException {
        fs.rename(new Path("/test/0415/bbb.txt"),new Path("/test/0415/bbb2.txt"));

        fs.close();
    }

    /**
     * 查看文件详情
     */
    @Test
    public void listFile() throws IOException {
        // 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus status = listFiles.next();
            // 文件名称
            System.out.println(status.getPath().getName());
            // 长度
            System.out.println(status.getLen());
            // 权限
            System.out.println(status.getPermission());
            // 分组
            System.out.println(status.getGroup());

            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();

            for (BlockLocation blockLocation : blockLocations) {

                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();

                for (String host : hosts) {
                    System.out.println(host);
                }
            }

            System.out.println("-------------------------");
        }
        fs.close();
    }


    /**
     * 判断是文件还是文件夹
     */
    @Test
    public void listStatus() throws IOException, InterruptedException, URISyntaxException{

        //判断是文件还是文件夹
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus : listStatus) {

            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("文件:"+fileStatus.getPath().getName());
            }else {
                System.out.println("文件夹:"+fileStatus.getPath().getName());
            }
        }

        fs.close();
    }


}
