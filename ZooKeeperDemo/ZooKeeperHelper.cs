using log4net;
using org.apache.zookeeper;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;

namespace ZooKeeperDemo
{
    public class ZooKeeperHelper
    {
        private List<string> _address;
        private int _sessiontTimeout = 10 * 1000;//10秒
        private ZooKeeper _zooKeeper;
        private int _connectTimeout = 3 * 30 * 1000;//每个zookeeper实例尝试连接最长30秒
        private string _success = "success";
        private string _fail = "fail";

        public ZooKeeperHelper(int sessionTimeOut = 10 * 1000)
        {
            _sessiontTimeout = sessionTimeOut;

            _address = new List<string> { "127.0.0.1:2181" };
        }

        /// <summary>
        /// 返回null表示连接不成功
        /// </summary>
        /// <param name="authEnum"></param>
        /// <param name="authInfo"></param>
        /// <returns></returns>
        public ZooKeeper Connect(AuthEnum authEnum, string authInfo)
        {
            foreach (string address in _address)
            {
                _zooKeeper = new ZooKeeper(address, _sessiontTimeout, new DefaultWatcher());
                if (authEnum != AuthEnum.world)
                {
                    _zooKeeper.addAuthInfo(authEnum.ToString(), System.Text.Encoding.UTF8.GetBytes(authInfo));
                }
                Stopwatch stopwatch = new Stopwatch();
                stopwatch.Start();
                while (stopwatch.ElapsedMilliseconds < _connectTimeout / _address.Count)
                {
                    ZooKeeper.States states = _zooKeeper.getState();
                    if (states == ZooKeeper.States.CONNECTED || states == ZooKeeper.States.CONNECTEDREADONLY)
                    {
                        break;
                    }
                }
                stopwatch.Stop();
                if (_zooKeeper.getState().ToString().ToUpper().Contains("CONNECTED"))
                {
                    break;
                }
            }

            return _zooKeeper;

        }

        /// <summary>
        /// 创建节点,不能在临时节点下创建子节点
        /// </summary>
        /// <param name="path">不要使用path等关键字作为路径</param>
        /// <param name="data"></param>
        /// <param name="persistent"></param>
        /// <returns></returns>
        public string CreateNode(string path, string data, bool persistent = false)
        {
            Task<string> task = _zooKeeper.createAsync(path, System.Text.Encoding.UTF8.GetBytes(data), ZooDefs.Ids.OPEN_ACL_UNSAFE, persistent ? CreateMode.PERSISTENT : CreateMode.EPHEMERAL);
            task.Wait();
            if (!string.IsNullOrEmpty(task.Result) && task.Status.ToString().ToLower() == "RanToCompletion".ToLower())
            {
                return task.Result;
            }

            return _fail;
        }

        /// <summary>
        /// 删除节点,删除节点的子节点个数必须为0，否则请先删除子节点
        /// </summary>
        /// <param name="path">不要使用path等关键字作为路径</param>
        /// <returns></returns>
        public string DeleteNode(string path)
        {
            Task task = _zooKeeper.deleteAsync(path);
            task.Wait();
            if (task.Status.ToString().ToLower() == "RanToCompletion".ToLower())
            {
                return _success;
            }

            return _fail;
        }

        /// <summary>
        /// 给节点设置数据
        /// </summary>
        /// <param name="path">不要使用path等关键字作为路径</param>
        /// <param name="data"></param>
        /// <returns></returns>
        public string SetData(string path, string data)
        {
            Task<org.apache.zookeeper.data.Stat> stat = _zooKeeper.setDataAsync(path, System.Text.Encoding.UTF8.GetBytes(data));
            stat.Wait();
            if (stat.Result != null && stat.Status.ToString().ToLower() == "RanToCompletion".ToLower())
            {
                return _success;
            }

            return _fail;
        }

        /// <summary>
        /// 判断节点是否存在
        /// </summary>
        /// <param name="path">不要使用path等关键字作为路径</param>
        /// <param name="watcher"></param>
        /// <returns></returns>
        public string ExistsNode(string path, Watcher watcher = null)
        {
            Task<org.apache.zookeeper.data.Stat> stat = _zooKeeper.existsAsync(path, watcher);
            stat.Wait();
            if (stat.Result != null && stat.Status.ToString().ToLower() == "RanToCompletion".ToLower())
            {
                return _success;
            }

            return _fail;
        }

        /// <summary>
        /// 得到节点相关信息
        /// </summary>
        /// <param name="path">不要使用path等关键字作为路径</param>
        /// <param name="watcher"></param>
        /// <returns></returns>
        public org.apache.zookeeper.data.Stat GetNode(string path, Watcher watcher = null)
        {

            Task<org.apache.zookeeper.data.Stat> stat = _zooKeeper.existsAsync(path, watcher);
            stat.Wait();
            if (stat.Result != null && stat.Status.ToString().ToLower() == "RanToCompletion".ToLower())
            {
                return stat.Result;
            }

            return null;
        }

        /// <summary>
        /// 得到节点数据
        /// </summary>
        /// <param name="path">不要使用path等关键字作为路径</param>
        /// <param name="watcher"></param>
        /// <returns></returns>
        public string GetData(string path, Watcher watcher = null)
        {
            Task<DataResult> dataResult = _zooKeeper.getDataAsync(path, watcher);
            dataResult.Wait();
            if (dataResult.Result != null && dataResult.Status.ToString().ToLower() == "RanToCompletion".ToLower())
            {
                return Encoding.UTF8.GetString(dataResult.Result.Data);
            }

            return _fail;
        }

        /// <summary>
        /// 得到后代节点路径
        /// </summary>
        /// <param name="path">不要使用path等关键字作为路径</param>
        /// <param name="watcher"></param>
        /// <returns></returns>
        public List<string> GetChildren(string path, Watcher watcher = null)
        {
            Task<ChildrenResult> childrenResult = _zooKeeper.getChildrenAsync(path, watcher);
            childrenResult.Wait();
            if (childrenResult.Result != null && childrenResult.Status.ToString().ToLower() == "RanToCompletion".ToLower())
            {
                return childrenResult.Result.Children;
            }

            return null;
        }

        /// <summary>
        /// 关闭连接
        /// </summary>
        /// <returns></returns>
        public string Close()
        {
            Task task = _zooKeeper.closeAsync();
            task.Wait();
            if (task.Status.ToString().ToLower() == "RanToCompletion".ToLower())
            {
                return _success;
            }

            return _fail;
        }

        /// <summary>
        /// 得到连接状态
        /// </summary>
        /// <returns></returns>
        public string GetState()
        {

            if (_zooKeeper != null)
            {
                ZooKeeper.States states = _zooKeeper.getState();
                return states.ToString();
            }

            return _fail;
        }

        /// <summary>
        /// 是否已经连接
        /// </summary>
        /// <returns></returns>
        public bool Connected()
        {
            if (_zooKeeper != null)
            {
                ZooKeeper.States states = _zooKeeper.getState();
                if (states == ZooKeeper.States.CONNECTED || states == ZooKeeper.States.CONNECTEDREADONLY)
                {
                    return true;
                }
            }

            return false;
        }
    }
}
