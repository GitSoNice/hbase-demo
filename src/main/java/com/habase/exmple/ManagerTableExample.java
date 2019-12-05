package com.habase.exmple;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * @author Colin
 * @Date 20191123
 * @Desc HBase创建表demo
 */
@Data
@NoArgsConstructor
public class ManagerTableExample {

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
    public void release(ManagerTableExample managerTableExample) throws IOException {
        managerTableExample.getAdmin().close();
        managerTableExample.getConnection().close();
    }

    /**
     * 创建表
     */
    public void createTable() throws IOException {
        TableDescriptor table = TableDescriptorBuilder.newBuilder(TableName.valueOf("t_class9"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("f1".getBytes())
                        .setMaxVersions(3)
                        .setInMemory(true)
                        .setBlocksize(8 * 1024)
                        .setScope(HConstants.REPLICATION_SCOPE_LOCAL)
                        .build())
                .build();
        byte[][] split = {Bytes.toBytes("100"),Bytes.toBytes("200"),Bytes.toBytes("500")};
        //直接建表
//        admin.createTable(table);
        //预分区建表
        admin.createTable(table,split);
    }

    /**
     * 查看所有表信息
     */
    public void showTables() throws IOException {
        List<TableDescriptor> tableDescriptors = admin.listTableDescriptors();
        for(TableDescriptor tableDescriptor : tableDescriptors){
            System.out.println(tableDescriptor.getTableName());
            System.out.println(tableDescriptor);
        }


    }

    /**
     * 删除表
     */
    private void deleteTalbe() throws IOException {
        TableName tableName = TableName.valueOf("t_class1");
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }

    /**
     * 修改表,添加列族
     */
    private void modifyTable() throws IOException {
        TableName tableName = TableName.valueOf("t_class");
        TableDescriptor tableDescriptor = admin.getDescriptor(tableName);
        tableDescriptor = TableDescriptorBuilder.newBuilder(tableDescriptor).setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("f5".getBytes())
                .setMaxVersions(3)
                .setInMemory(true)
                .setBlocksize(8 * 1024)
                .setScope(HConstants.REPLICATION_SCOPE_LOCAL)
                .build())
                .build();
        admin.modifyTable(tableDescriptor);

    }

    /**
     * 查看表描述
     * @throws IOException
     */
    public void getTableDescribe() throws IOException {
        TableDescriptor table = admin.getDescriptor(TableName.valueOf("t_user"));
        ColumnFamilyDescriptor[] columnFamilies = table.getColumnFamilies();
        for (ColumnFamilyDescriptor hcd : columnFamilies) {
            System.out.println("HColumn: "+ Bytes.toString(hcd.getName()));
        }
    }

    public static void main(String[] args) throws IOException {
        ManagerTableExample managerTableExample = new ManagerTableExample();
        managerTableExample.init();
        //创建表
        managerTableExample.createTable();
        //查看所有表
//        managerTableExample.showTables();
        //删除表
//        managerTableExample.deleteTalbe();
        //查看所有表
        managerTableExample.showTables();
        //修改表
//        managerTableExample.modifyTable();
        //查看表描述
        managerTableExample.getTableDescribe();
        managerTableExample.release(managerTableExample);
    }

}
