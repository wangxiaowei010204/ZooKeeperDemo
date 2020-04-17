using log4net;
using org.apache.zookeeper;
using System;
using System.Collections.Generic;
using System.Text;

namespace ZooKeeperDemo
{
    public class TaskHelper
    {
        private int forcerun = 0;
        private string servicename = "ZooKeeperTest";
        private string pathname = "ZooKeeperBase";
        private string childpathname = "ZooKeeperChild";
        private ZooKeeperHelper _zooKeeperHelper;
        private ZooKeeper _zooKeeper;

        //单例输出，否则通知过多可能导致内存溢出
        private static TaskHelper _taskHelper;
        private static object _obj = new object();

        private TaskHelper( int sessionTimeOut = 10 * 1000)
        {
            _zooKeeperHelper = new ZooKeeperHelper( sessionTimeOut);
        }

        /// <summary>
        /// 创建zookeeper实例
        /// </summary>
        /// <param name="log"></param>
        /// <param name="sessionTimeOut"></param>
        /// <returns></returns>
        public static TaskHelper GetInstance(int sessionTimeOut = 10 * 1000)
        {
            if (_taskHelper == null)
            {
                lock (_obj)
                {
                    if (_taskHelper == null)
                    {
                        _taskHelper = new TaskHelper( sessionTimeOut);
                    }
                }
            }
            return _taskHelper;
        }

        public bool Return()
        {
            if (forcerun != 1)
            {
                try
                {
                    if (!_zooKeeperHelper.Connected())
                    {
                        _zooKeeper = _zooKeeperHelper.Connect(AuthEnum.world, "");
                        if (_zooKeeper == null)
                        {
                            Console.WriteLine("连接zooKeeper失败，时间是：{0}", DateTime.Now);
                            return true;
                        }
                    }
                    string path = ("/" + pathname);
                    string data = servicename;
                    string str = _zooKeeperHelper.ExistsNode(path, new NodeWatcher());
                    if (str != "success")
                    {
                        str = _zooKeeperHelper.CreateNode(path, data);
                        if (str != path)
                        {
                            Console.WriteLine("创建路径失败，时间是：{0}", DateTime.Now);
                            return true;
                        }
                    }

                    string lockname = _zooKeeperHelper.GetData(path, new NodeWatcher());

                    #region 测试通知

                    string cg = _zooKeeperHelper.SetData(path, "hahhahahah");
                    cg = _zooKeeperHelper.GetData(path, new NodeWatcher());
                    cg = _zooKeeperHelper.SetData(path, "1111111111");
                    cg = _zooKeeperHelper.GetData(path, new NodeWatcher());
                    cg = _zooKeeperHelper.DeleteNode(path);

                    #endregion

                    //执行标识
                    if (lockname != servicename)
                    {
                        Console.WriteLine("非工作时间，当前执行的服务是：{0}，时间是：{1}", lockname, DateTime.Now);
                        return true;
                    }
                }
                catch (Exception exception)
                {
                    Console.WriteLine("zooKeeperHelper出现异常：{0}，时间是：{1}", exception.Message + exception.StackTrace, DateTime.Now);
                }
            }
            return false;
        }
    }


}
