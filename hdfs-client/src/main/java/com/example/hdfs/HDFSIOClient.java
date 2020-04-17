package com.example.hdfs;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;

/**
 * @className: HDFSClient.java
 * @package com.example.hdfs
 * @description:
 * @date 2020/4/15 15:36
 */
public class HDFSIOClient extends AbstractHdfsConfig{

    /**
     * 文件上传
     */
    @Test
    public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {
        // 1 创建输入流
        FileInputStream fis = new FileInputStream(new File("C:/Users/ALIENWARE/Desktop/aaa.txt"));

        // 2 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/test/0415/aaa.txt"));

        // 3 拷贝流
        IOUtils.copyBytes(fis, fos, configuration);

        // 4 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }


    /**
     * 文件下载
     */
    @Test
    public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException{
        // 1 获取输入流
        FSDataInputStream fis = fs.open(new Path("/test/0415/aaa.txt"));

        // 2 获取输出流
        FileOutputStream fos = new FileOutputStream(new File("C:/Users/ALIENWARE/Desktop/aaaFromHdfs.txt"));

        // 3 流的对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 4 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    /**
     * 定位文件读取 读取第一块
     */
    @Test
    public void readFileSeek1() throws IOException, InterruptedException, URISyntaxException{
        // 1 获取输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

        // 2 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("F:/hadoop-2.7.2.tar.gz.part1"));

        // 3 流的拷贝
        byte[] buf = new byte[1024];

        for(int i =0 ; i < 1024 * 128; i++){
            fis.read(buf);
            fos.write(buf);
        }

        // 5关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    /**
     * 定位文件读取 读取第二块
     */
    @Test
    public void readFileSeek2() throws IOException, InterruptedException, URISyntaxException{
        // 1 打开输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

        // 2 定位输入数据位置
        fis.seek(1024*1024*128);

        // 3 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("F:/hadoop-2.7.2.tar.gz.part2"));

        // 4 流的对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }

}
