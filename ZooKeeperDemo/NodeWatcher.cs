using log4net;
using org.apache.zookeeper;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace ZooKeeperDemo
{
    /// <summary>
    /// 节点监听
    /// </summary>
    public class NodeWatcher : Watcher
    {
        private string _state;
        private Event.EventType _type;
        private string _path;
        internal static readonly Task CompletedTask = Task.FromResult(1);

        public NodeWatcher()
        {
        }

        public override Task process(WatchedEvent @event)
        {
            _state = @event.getState().ToString();
            _type = @event.get_Type();
            _path = @event.getPath();
            switch (_type)
            {
                case Event.EventType.NodeCreated:
                    HandleCreate();
                    break;
                case Event.EventType.NodeDeleted:
                    HandleDelete();
                    break;
                case Event.EventType.NodeDataChanged:
                    HandleDataChange();
                    break;
                case Event.EventType.NodeChildrenChanged:
                    HandleChildrenChange();
                    break;
                default:
                    HandleNone();
                    break;
            }
            return CompletedTask;
        }


        /// <summary>
        /// 创建节点事件
        /// </summary>
        private void HandleCreate()
        {
             Console.WriteLine("NodeCreated");
        }

        private void HandleDelete()
        {
            Console.WriteLine("NodeDeleted");
        }

        private void HandleDataChange()
        {
            Console.WriteLine("NodeDataChanged");
        }

        private void HandleChildrenChange()
        {
            Console.WriteLine("NodeChildrenChanged");
        }

        private void HandleNone()
        {
            Console.WriteLine(_state);
        }
    }


}
