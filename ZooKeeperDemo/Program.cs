using log4net;
using System;

namespace ZooKeeperDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            TaskHelper.GetInstance().Return();

            Console.WriteLine("测试完成");
            Console.ReadKey();
        }
    }
}
