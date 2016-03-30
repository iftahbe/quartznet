using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Quartz.Impl.RavenDB
{
    public class SimpleKey
    {
        public string Name { get; set; }
        public string Group { get; set; }

        public SimpleKey(string name, string group)
        {
            Name = name;
            Group = group;
        }
    }
}
