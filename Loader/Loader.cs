using Microsoft.VisualBasic.FileIO;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Loader
{
    public static class Loader
    {
        public static void Load(string conf)
        {
            //string loader = @"C:\Users\Elia\Downloads\FYP_Data\Loader_Incoming";
            using (SqlConnection connection = new SqlConnection(ParamsHelper.connectionStringData))
            {
                connection.Open();
                string fileMask = conf.Contains("ZTE_PM") ? "*ZTE_PM*.csv" : conf.Contains("ZTE_FM") ? "*ZTE_FM*.csv" : conf.Contains("ZTE_CM") ? "*ZTE_CM*.csv" : conf.Contains("JUN_CM") ? "*JUN_CM*.csv" : conf.Contains("JUN_FM") ? "*JUN_FM*.csv": conf.Contains("JUN_PM") ? "*JUN_PM*.csv" : "";
                string[] files = Directory.GetFiles(ParamsHelper.Loader_inputFolder, fileMask);
                //int filecount = 0;
                foreach (string file in files)
                {
                    //while (filecount < 200)
                    //{
                    string tableName = GetTableNameFromFile(file, conf);
                    if (tableName != null)
                    {
                        DataTable dataTable = LoadDataFromFile(file);
                        if (dataTable != null)
                        {
                            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
                            {
                                bulkCopy.DestinationTableName = tableName;
                                bulkCopy.BatchSize = 2000;
                                bulkCopy.WriteToServer(dataTable);
                            }
                            string destinationFilePath = Path.Combine(ParamsHelper.Loader_outputFolder, Path.GetFileName(file));
                            File.Move(file, destinationFilePath);
                            //filecount++;
                        }
                    }
                }
            }
            //}
            Environment.Exit(0);
        }

        static string GetTableNameFromFile(string filePath, string conf)
        {
            string fileName = Path.GetFileNameWithoutExtension(filePath);

            if (conf.Contains("ZTE_FM_LOADER"))
            {
                return "TRANS_MW_ZTE_FM";
            }
            else if (conf.Contains("ZTE_CM_LOADER"))
            {
                if (fileName.Contains("_AIR_"))
                    return "TRANS_MW_ZTE_CM_AIR";
                else if (fileName.Contains("_AIRLINK"))
                    return "TRANS_MW_ZTE_CM_AIRLINK";
                else if (fileName.Contains("_BOARD"))
                    return "TRANS_MW_ZTE_CM_BOARD";
                else if (fileName.Contains("_ETHERNET"))
                    return "TRANS_MW_ZTE_CM_ETHERNET";
                else if (fileName.Contains("_MICROWAVE"))
                    return "TRANS_MW_ZTE_CM_MICROWAVE";
                else if (fileName.Contains("_NEINFO"))
                    return "TRANS_MW_ZTE_CM_NEINFO";
                else if (fileName.Contains("_PLA"))
                    return "TRANS_MW_ZTE_CM_PLA";
                else if (fileName.Contains("_TOPLINK"))
                    return "TRANS_MW_ZTE_CM_TOPLINK";
                else if (fileName.Contains("_TU"))
                    return "TRANS_MW_ZTE_CM_TU";
                else if (fileName.Contains("_XPIC"))
                    return "TRANS_MW_ZTE_CM_XPIC";
                else if (fileName.Contains("_IM_"))
                    return "TRANS_MW_ZTE_CM_INV";

                else return null;
            }
            else if (conf.Contains("ZTE_PM_LOADER"))
            {
                if (fileName.Contains("ACM_"))
                    return "TRANS_MW_ZTE_PM_ACM";
                else if (fileName.Contains("ENV_"))
                    return "TRANS_MW_ZTE_PM_ENV";
                else if (fileName.Contains("ODU_"))
                    return "TRANS_MW_ZTE_PM_ODU";
                else if (fileName.Contains("RMONQOS_"))
                    return "TRANS_MW_ZTE_PM_RMONQOS";
                else if (fileName.Contains("TRAFFICUNITRADIOLINKPERFORMANCE_"))
                    return "TRANS_MW_ZTE_PM_TRAFFICUNITRADIOLINKPERFORMANCE";
                else if (fileName.Contains("WE_"))
                    return "TRANS_MW_ZTE_PM_WE";
                else if (fileName.Contains("WETH_"))
                    return "TRANS_MW_ZTE_PM_WETH";
                else if (fileName.Contains("WL_"))
                    return "TRANS_MW_ZTE_PM_WL";
                else if (fileName.Contains("XPIC_"))
                    return "TRANS_MW_ZTE_PM_XPIC";

                else return null;
            }
            else if (conf.Contains("JUN_CM_LOADER"))
            {
                if (fileName.Contains("BRIDGE_DOMAIN_INFO"))
                    return "TRANS_IP_JUN_CM_BRIDGE_DOMAIN_INFO";
                else if (fileName.Contains("DEVICE_INFO"))
                    return "TRANS_IP_JUN_CM_DEVICE_INFO";
                else if (fileName.Contains("HARDWARE_INFO"))
                    return "TRANS_IP_JUN_CM_HARDWARE_INFO";
                else if (fileName.Contains("INTERFACE_INFORMATION"))
                    return "TRANS_IP_JUN_CM_INTERFACE_INFORMATION";
                else if (fileName.Contains("INTERFACE_STATUS"))
                    return "TRANS_IP_JUN_CM_INTERFACE_STATUS";
                else if (fileName.Contains("ISIS_ADJ"))
                    return "TRANS_IP_JUN_CM_ISIS_ADJ_INFO";
                else if (fileName.Contains("ISIS_SPF"))
                    return "TRANS_IP_JUN_CM_ISIS_SPF_BRIEF_INFO";
                else if (fileName.Contains("NETWORK_INFO"))
                    return "TRANS_IP_JUN_CM_NETWORK_INFO";
                else if (fileName.Contains("ROUTING_INFO"))
                    return "TRANS_IP_JUN_CM_ROUTING_INFO";
                else return null;
            }
            else if (conf.Contains("JUN_FM_LOADER"))
            {
                return "TRANS_IP_JUN_FM_ALARMLOG";
            }
            else if (conf.Contains("JUN_PM_LOADER"))
            {
                if (fileName.Contains("ROUTER_HOST"))
                    return "TRANS_IP_JUN_PM_ROUTER_HOST";
                else return null;
            }
            else
            {
                Console.WriteLine($"Skipping file: {filePath} - Unknown file type.");
                return null;
            }

        }
        static DataTable LoadDataFromFile(string filePath)
        {
            DataTable dataTable = new DataTable();

            using (TextFieldParser parser = new TextFieldParser(filePath))
            {
                parser.TextFieldType = FieldType.Delimited;
                parser.SetDelimiters(",");
                parser.HasFieldsEnclosedInQuotes = false;

                bool isColumnRow = true;
                int columnCount = 0;

                while (!parser.EndOfData)
                {
                    string[] fields = parser.ReadFields();

                    if (isColumnRow)
                    {
                        isColumnRow = false;
                        columnCount = fields.Length;

                        foreach (string columnName in fields)
                        {
                            dataTable.Columns.Add(columnName);
                        }
                    }
                    else
                    {
                        if (fields.Length == columnCount)
                        {
                            dataTable.Rows.Add(fields);
                        }
                        else
                        {
                            Console.WriteLine($"Skipping invalid row in file: {filePath} - Number of values doesn't match the number of columns.");
                            
                        }
                    }
                }
            }
            return dataTable;
        }
    }
}
