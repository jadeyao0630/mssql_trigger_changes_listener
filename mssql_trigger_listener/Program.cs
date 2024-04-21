


using mssql_trigger_listener;
using System.Timers;


class Program
{
    static void Main(string[] args)
    {
        Console.WriteLine("参数数量: " + args.Length);
        var isInit = false;
        for (int i = 0; i < args.Length; i++)
        {
            Console.WriteLine($"参数 {i + 1}: {args[i]}");
            if (args[i] == "--init" || args[i] == "-i") isInit = true;
            if (args[i] == "--help" || args[i] == "-h")
            {
                ShowHelp();
                return;
            }
        }
        var parser = new IniFileParser();
        try
        {
            var iniData = parser.Parse("config.ini");
            var Mssql = "MsSqlServer";
            var MssqlTarget = "MsSqlServerTarget";


            if (iniData.ContainsKey(Mssql) && iniData.ContainsKey(MssqlTarget))
            {
                var server = iniData[Mssql];
                var server_target = iniData[MssqlTarget];
                if (server != null &&
                    server.ContainsKey("Server") &&
                    server.ContainsKey("Database") &&
                    server.ContainsKey("User") &&
                    server.ContainsKey("Password") &&
                    server_target != null &&
                    server_target.ContainsKey("Server") &&
                    server_target.ContainsKey("User") &&
                    server_target.ContainsKey("Password"))
                {
                    var port = server.ContainsKey("Port") ? server["Port"] : "1433";
                    var port_target = server.ContainsKey("Port") ? server_target["Port"] : "1433";
                    //var data = $"server={server["Server"]},{port};Database={server["Database"]};User Id={server["User"]};Password={server["Password"]};Trusted_Connection=True";
                    mssqlChangeListerner listener = new mssqlChangeListerner(
                        new database
                        {
                            server = server["Server"],
                            port = int.Parse(port),
                            databaseName = server["Database"],
                            user = server["User"],
                            password = server["Password"],
                            isTrusted = server.ContainsKey("isTrusted") && server["isTrusted"] == "1",

                        },
                        new database
                        {
                            server = server_target["Server"],
                            port = int.Parse(port_target),
                            user = server_target["User"],
                            password = server_target["Password"],
                            isTrusted = server_target.ContainsKey("isTrusted") && server_target["isTrusted"] == "1",
                        }
                    );

                    if (iniData.ContainsKey("General"))
                    {
                        var general = iniData["General"];
                        Console.WriteLine(general.ContainsKey("Tables"));
                        if (general.ContainsKey("Tables") && !string.IsNullOrEmpty(general["Tables"]))
                        {
                            listener.Start(general["Tables"].Split(','), isInit);
                        }
                        else
                        {
                            Console.WriteLine("tables have not been defineded...");
                        }

                    }

                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
            return;
        }

    }
    static void ShowHelp()
    {
        // 帮助说明文本
        string helpText = @"
Usage: mssql_listener.exe [options]

Options:
  --help, -h      Show this help message and exit
  --init, -i      Inital the target databse when start the application

Description:
  This is a C# console application designed to synchronize data from a source SQL Server to a target SQL Server.
";

        Console.WriteLine(helpText);
    }
}



