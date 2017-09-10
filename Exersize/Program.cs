using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Threading;
using System.Security.Cryptography;
using System.Data.OracleClient;

namespace Exersize
{
    internal struct HashField  //формат хэш-данных
    {
        public string file;
        public string hash;

        public HashField(string curFile, string curHash)
        {
            file = curFile;
            hash = curHash;
        }
        
    }

    class Program
    {
        private static bool workIsDone = false;        //флаги о завершении работы методов
        private static bool fileSearchIsDone = false;
        private static bool hashComputeIsDone = false;
        private static bool writeToDbIsDone = false;

        private static bool FounDedF = false; // флаг нахождения файла

        private static object fileListLocker = new object(); //маркеры для блокировки доступа к разделяемым ресурсам
        private static object hashListLocker = new object();

        private static string workDirectory = string.Format(@"C:\TEST");      //путь к папке
        private static Queue<string> fileList = new Queue<string>();                        //список файлов
        private static Queue<HashField> hashList = new Queue<HashField>();                  //список хэшей
        private static int fileCount = 0;                                                   //количество файлов
        private static int completedFileCount = 0;
        static OracleConnection newconnect = new OracleConnection("DATA SOURCE=localhost:1521/xe;PASSWORD=123456;PERSIST SECURITY INFO=True;USER ID=tagrisen");//количество обработанных файлов

        delegate void DelFileSearch(string path);   //делегаты к методам
        delegate void DelHashCompute();
        delegate void DelWriteToDb();

        static void Main(string[] args)
        {
            newconnect.Open();



            DelFileSearch dfs = new DelFileSearch(FileSearch);                 //экземпляр делегата для метода поиска файлов
            AsyncCallback dfsCallBack = new AsyncCallback(FileSearchComplete); //экземпляр делегата для обратно вызываемого метода по завершении работы dfs
            dfs.BeginInvoke(workDirectory, dfsCallBack, null);                 //соответственно по завершении dfs будет вызван метод FileSearchComplete

            DelHashCompute dhc = new DelHashCompute(HashCompute);
            AsyncCallback dhcCallBack = new AsyncCallback(HashComputeComplete);
            dhc.BeginInvoke(dhcCallBack, null);

            DelWriteToDb dwdb = new DelWriteToDb(WriteToDb);
            AsyncCallback dwdbCallBack = new AsyncCallback(WriteToDbComplete);
            dwdb.BeginInvoke(dwdbCallBack, null);
            

           
            Console.ReadLine();

        }

        //Метод поиска файлов
        private static void FileSearch(string path)
        {
            try
            {
                foreach (string file in Directory.EnumerateFiles(path, "*.*", SearchOption.AllDirectories))
                {
                    Console.WriteLine("Found " + file);
                    lock (fileListLocker)
                    {
                        fileList.Enqueue(file);
                        FounDedF = true;
                    }
                    fileCount++;
                    Thread.Sleep(10);
                }
            }
            catch(DirectoryNotFoundException e)//если не найдена папка, выводится исключение и также записывается в логи БД
            {
                
                Console.WriteLine(e.Message);
                OracleCommand OR1insert = new OracleCommand();
                OR1insert.Connection = newconnect;
                string query = "INSERT INTO ERRORLOGS(ERR_DATE, EXCEPTION) VALUES((SELECT SYSDATE FROM DUAL), '" + e.Message + "')";
                OR1insert.CommandText = query;
                OR1insert.ExecuteNonQuery();

                OR1insert = null;
                
            }
        }

        //метод для обработки завершения потока поиска файлов
        private static void FileSearchComplete(IAsyncResult result)
        {
            fileSearchIsDone = true;
            if (FounDedF == false)
            {
                Console.WriteLine("Files not found");
                OracleCommand OR2insert = new OracleCommand();
                OR2insert.Connection = newconnect;
                string query = "INSERT INTO ERRORLOGS(ERR_DATE, EXCEPTION) VALUES((SELECT SYSDATE FROM DUAL), 'Файлы не обнаружены')";
                OR2insert.CommandText = query;
                OR2insert.ExecuteNonQuery();

                OR2insert = null;

            }
            Console.WriteLine("File seraching thread is complete");
        }

        //метод вычисляющий хэш
        private static void HashCompute()
        {
            while((!fileSearchIsDone) || (fileList.Count != 0))
            {
                if (fileList.Count != 0)
                {
                    string curFile = null;
                    string curHash = null;

                    lock(fileListLocker)
                    {
                        curFile = fileList.Dequeue();
                    }
                    using (FileStream curFileStream = File.OpenRead(curFile))
                    {
                        byte[] data = new byte[curFileStream.Length];
                        curFileStream.Read(data, 0, (int)curFileStream.Length);
                        byte[] checkSum = new MD5CryptoServiceProvider().ComputeHash(data);
                        curHash = BitConverter.ToString(checkSum).Replace("-", String.Empty);
                    }
                    HashField curHashField = new HashField(curFile, curHash);
                    lock(hashListLocker)
                    {
                        hashList.Enqueue(curHashField);
                    }
                    Console.WriteLine("Compute hash " + curFile);
                    //completedFileCount++;
                }
            }
        }

        //метод для обработки завершения потока вычислений хэшей
        private static void HashComputeComplete(IAsyncResult result)
        {
            hashComputeIsDone = true;
            Console.WriteLine("Hash computing thread is complete");
        }

        //метод записи в БД
        private static void WriteToDb()
        {
            
            while (!hashComputeIsDone || hashList.Count != 0)
            {
                if (hashList.Count != 0)
                {
                    HashField hf = new HashField();
                    lock (hashListLocker)
                    {
                        hf = hashList.Dequeue();
                    }
                    OracleCommand ORinsert = new OracleCommand();
                    ORinsert.Connection = newconnect;
                    string query = "INSERT INTO HASHINFO(FILENAME, MD5HASH) VALUES('" + hf.file + "', '"+ hf.hash+"')";
                    ORinsert.CommandText = query;
                    ORinsert.ExecuteNonQuery();
                    Console.WriteLine("Database write " + hf.file+" "+ hf.hash);
                    completedFileCount++;

                    ORinsert = null;
                }
            }
            
        }

        //метод для обработки завершения работы потока записи в БД
        private static void WriteToDbComplete(IAsyncResult result)
        {
            writeToDbIsDone = true;
            Console.WriteLine("DataBase writing thread is complete");
        }
    }
}
