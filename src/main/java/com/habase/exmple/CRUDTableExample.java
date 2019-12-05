package com.habase.exmple;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Colin
 * @Date 2019-11-24
 * @Desc 表的增删改查
 */
@Data
@NoArgsConstructor
public class CRUDTableExample {


    private Connection connection;

    private Admin admin;

    /**
     * 链接
     * @throws IOException
     */
    public void init() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","192.168.1.233");
        configuration.set("hbase.zookeeper.property.clientPort","2181");
        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();
    }

    /**
     * 释放链接资源
     * @param managerTableExample
     * @throws IOException
     */
    public void release(CRUDTableExample managerTableExample) throws IOException {
        managerTableExample.getAdmin().close();
        managerTableExample.getConnection().close();
    }

    /**
     * 插入数据若存在默认更新
     * @param i
     * @throws IOException
     */
    public void insertData(int i) throws IOException {
        TableName tableName = TableName.valueOf("t_class1");
        Table table = connection.getTable(tableName);
        List<Put> userList = new ArrayList<>();

        //批量插入
        for (; i >0; i--) {
            Put row = new Put(Bytes.toBytes(String.valueOf(i)));
            row.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes("bobo"+i));
//            row.addColumn(Bytes.toBytes("f5"), Bytes.toBytes("age"), Bytes.toBytes(String.valueOf(i)));
            row.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("address"), Bytes.toBytes("address"+i));
            userList.add(row);
        }
        System.out.println(userList.size());
        table.put(userList);

        //单个插入  --key区分大小写
        Put user = new Put(Bytes.toBytes("colin"));
        user.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("realName"),Bytes.toBytes("Alex"));
        Put userUpperCase = new Put(Bytes.toBytes("colin"));
        userUpperCase.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("REALNAME"),Bytes.toBytes("ALEX"));
        userUpperCase.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("realname"),Bytes.toBytes("alex"));
        table.put(user);
        table.put(userUpperCase);
    }

    /**
     * 当value值与数据库中的值一致，put数据
     */
    private void checkAndputData() throws IOException {
        TableName tableName = TableName.valueOf("t_class1");
        Table table = connection.getTable(tableName);
        Put userUpperCase = new Put(Bytes.toBytes("colin"));
        userUpperCase.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("REALNAME"),Bytes.toBytes("alex"));
        boolean b = table.checkAndMutate(Bytes.toBytes("colin"), Bytes.toBytes("f1"))
                .qualifier(Bytes.toBytes("realname"))
                .ifEquals(Bytes.toBytes("ALEX")).thenPut(userUpperCase);
        System.out.println(b);
    }

    /**
     * 删除数据
     * @throws IOException
     */
    private void deleteData() throws IOException {
        TableName tableName = TableName.valueOf("t_class1");
        Table table = connection.getTable(tableName);
        Delete delete = new Delete(Bytes.toBytes("colin"));
        delete.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("REALNAME"));
        //传一个List<Delete>就是批量删除
        table.delete(delete);
    }

    /**
     * 删除整行
     * @throws IOException
     */
    private void deleteAll() throws IOException {
        TableName tableName = TableName.valueOf("t_class1");
        Table table = connection.getTable(tableName);
        Delete delete = new Delete(Bytes.toBytes("colin"));
        table.delete(delete);
    }

    /**
     * scan全表扫描
     * @throws IOException
     */
    private void scanTable() throws IOException {
        TableName tableName = TableName.valueOf("t_class1");
        Table table = connection.getTable(tableName);
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for(Result result : scanner){
            CellScanner cell = result.cellScanner();
            while(cell.advance()){
                Cell current = cell.current();

                System.out.println("rowkey:"+new String(current.getRowArray(), current.getRowOffset(), current.getRowLength())+
                        " family:" + new String(current.getFamilyArray(), current.getFamilyOffset(), current.getFamilyLength()) +
                        " keys:" + new String(current.getQualifierArray(), current.getQualifierOffset(), current.getQualifierLength()) +
                        " valie:"+ new String(current.getValueArray(), current.getValueOffset(), current.getValueLength()));
            }

        }

    }

    /**
     * 通过rowkey查找
     */
    private void findByRowkey() throws IOException {
        TableName tableName = TableName.valueOf("t_user");
        Get get = new Get(Bytes.toBytes("1"));
        Table table = connection.getTable(tableName);
        //可以传List<Get>
        Result result = table.get(get);
        CellScanner cell = result.cellScanner();
        while(cell.advance()) {
            Cell current = cell.current();
            System.out.println("rowkey:"+new String(current.getRowArray(), current.getRowOffset(), current.getRowLength())+
                    " family:" + new String(current.getFamilyArray(), current.getFamilyOffset(), current.getFamilyLength()) +
                    " keys:" + new String(current.getQualifierArray(), current.getQualifierOffset(), current.getQualifierLength()) +
                    " value:"+ new String(current.getValueArray(), current.getValueOffset(), current.getValueLength()));
        }
    }

    /**
     * 查看某个具体值，并指定版本
     * @throws IOException
     */
    private void findOne() throws IOException {
        TableName tableName = TableName.valueOf("t_user");
        Table table = connection.getTable(tableName);
        Get get = new Get(Bytes.toBytes("1"));
        get.addColumn(Bytes.toBytes("f5"),Bytes.toBytes("age"));
        //显示所有版本
//        get.readAllVersions();
        //显示指定版本
        get.readVersions(1);
        //指定时间戳
//        get.setTimestamp(1574562857489L);
        Result result = table.get(get);
        CellScanner cellScanner = result.cellScanner();
        while(cellScanner.advance()){
            Cell current = cellScanner.current();
            System.out.println("rowkey:"+new String(current.getRowArray(), current.getRowOffset(), current.getRowLength())+
                    " family:" + new String(current.getFamilyArray(), current.getFamilyOffset(), current.getFamilyLength()) +
                    " keys:" + new String(current.getQualifierArray(), current.getQualifierOffset(), current.getQualifierLength()) +
                    " value:"+ new String(current.getValueArray(), current.getValueOffset(), current.getValueLength()));
        }
    }


    public static void main(String[] args) throws IOException {
        CRUDTableExample crudTableExample = new CRUDTableExample();
        crudTableExample.init();
        //新增数据若存在默认更新
//        long l = System.currentTimeMillis();
//        crudTableExample.insertData(5);
//        System.out.println("end"+String.valueOf(System.currentTimeMillis()-l));

        //checkAndput，只有预期的value值与数据库中一致，才能够put
//        crudTableExample.checkAndputData();
        //删除数据
//      crudTableExample.deleteData();
        //删除整行
//        crudTableExample.deleteAll();
        //scan扫描全表
//        crudTableExample.scanTable();
        //根据rowkey查找
//        crudTableExample.findByRowkey();
        //查找具体key的值
        crudTableExample.findOne();

        crudTableExample.release(crudTableExample);
    }


}
