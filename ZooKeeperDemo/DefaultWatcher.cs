using log4net;
using org.apache.zookeeper;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace ZooKeeperDemo
{
    /// <summary>
    /// 默认监听
    /// </summary>
    public class DefaultWatcher : Watcher
    {
        internal static readonly Task CompletedTask = Task.FromResult(1);

        public DefaultWatcher()
        {
        }

        /// <summary>
        /// 接收通知
        /// </summary>
        /// <param name="event"></param>
        /// <returns></returns>
        public override Task process(WatchedEvent @event)
        {
            Console.WriteLine("接收到ZooKeeper服务端的通知，State是：{0}，EventType是：{1}，Path是：{2}", @event.getState(), @event.get_Type(), @event.getPath() ?? string.Empty);

            return CompletedTask;
        }
    }
}
