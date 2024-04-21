using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace mssql_trigger_listener
{
    internal class IniFileParser
    {
        public Dictionary<string, Dictionary<string, string>> Parse(string filePath)
        {
            var iniData = new Dictionary<string, Dictionary<string, string>>();
            string currentSection = null;

            foreach (var line in File.ReadAllLines(filePath))
            {
                string trimmedLine = line.Trim();
                if (trimmedLine.StartsWith(";") || string.IsNullOrEmpty(trimmedLine))
                {
                    // 忽略注释和空行
                    continue;
                }

                if (trimmedLine.StartsWith("[") && trimmedLine.EndsWith("]"))
                {
                    // 新节
                    currentSection = trimmedLine.Trim('[', ']');
                    iniData[currentSection] = new Dictionary<string, string>();
                }
                else if (currentSection != null)
                {
                    // 键值对
                    var keyValue = trimmedLine.Split(new char[] { '=' }, 2);
                    if (keyValue.Length == 2)
                    {
                        iniData[currentSection][keyValue[0].Trim()] = keyValue[1].Trim();
                    }
                }
            }

            return iniData;
        }
    }
}