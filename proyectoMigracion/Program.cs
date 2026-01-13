using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;


namespace MigrationApp
{
    // Clase para representar cada elemento del JSON
    public class MigrationItem
    {
        //JsonPropertyName dice que cuando lea el JSON debe coger la propiedad de la clase de la clave csv 
        [JsonPropertyName("csv")] public String Csv { get; set; }
        [JsonPropertyName("tabla")] public String Tabla { get; set; }


    }

    class Program
    {
        static void Main(string[] args)
        {
            var baseDir = AppContext.BaseDirectory;
            // Ruta del archivo JSON
            //ruta absoluta
            //string jsonPath = "migration.json";
            //ruta relativa
            var jsonPath = Path.Combine(baseDir, "migration.json");

            // Ruta de los archivos CSV
            //ruta absoluta
             //string CsvPath = "C:\\Users\\sergio.paredes-beca\\source\\repos\\proyectoMigracion\\proyectoMigracion\\bin\\Debug\\net8.0\\csv";
            //ruta relativa
            var CsvPath = Path.Combine(baseDir, "csv");

            //Conexion SQL Server
            //    string connString = " Server = (localdb)\\MSSQLLocalDB; Database = Arc.ArchetypeServices.DB;User Id=sa; TrustServerCertificate=True";
            string connString = "Server=tcp:localhost,1433;Database=Arc.ArchetypeServices.DB;User Id=sa;Password=Pass@word;TrustServerCertificate=True;";
            //comprueba si existe o no el archivo del JSON
            if (!File.Exists(jsonPath))
            {
                Console.WriteLine("El archivo migration.json no existe.");
                return;
            }

            //Se comprueba si existe o no la carpeta CSV
            if (!Directory.Exists(CsvPath))
            {
                Console.WriteLine("La carpeta no existe: " + CsvPath);
            }


            // Leer el contenido del JSON
            string jsonContent = File.ReadAllText(jsonPath);

            // Deserializar a un diccionario
            //ignora mayusculas y minusculas
            var option = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
            //convierte el contenido de un JSON a un dicionario 
            var migrationConfig = JsonSerializer.Deserialize<Dictionary<string, MigrationItem>>(jsonContent, option);

            //si esta vacio o no tiene nada 
            if (migrationConfig == null || migrationConfig.Count == 0)
            {

                Console.WriteLine("No se encontraron configuraciones en el JSON.");
                return;
            }

            //Conexion a SQL Server
            using var conn = new SqlConnection(connString);
            conn.Open();

            // Archivos en carpeta (nombre base)
            var nombresEnCarpeta = Directory.GetFiles(CsvPath) //obtiene todas las rutas completas de los archivos en la carpeta csvDir 
                                            .Select(Path.GetFileName) //obtiene el nombre del archivo sin la ruta completa
                                            .ToHashSet(StringComparer.OrdinalIgnoreCase); // ignora mayusculas y minusculas 

            //Recorre la configuracion del JSON 
            //migrationConfig es un Dictionary<string, MigrationItem> id es la clave del diccionario id=01 e item es el objeto con Csv y Tabla/ se limpia con el trim
            foreach (var (id, item) in migrationConfig)
            {
                var tabla = (item.Tabla ?? "").Trim();
                var csvName = (item.Csv ?? "").Trim();

                //valida que el registro JSON tiene datos 
                if (string.IsNullOrWhiteSpace(tabla))
                {
                    Console.WriteLine($"[{id}]'tabla' vacío en JSON. Se omite.");
                    continue;
                }
                //valida que el registro csv tiene datos
                if (string.IsNullOrWhiteSpace(csvName))
                {
                    Console.WriteLine($"[{id}]'csv' vacío en JSON. Se omite.");
                    continue;
                }
                //pone la extension.csv
                if (!csvName.EndsWith(".csv", StringComparison.OrdinalIgnoreCase))
                    csvName += ".csv";

                //pinta el id , el csv y la tabla
                Console.WriteLine($"[{id}] CSV: '{csvName}' → Tabla: '{tabla}'");


                //comprobacion que ese CSV esta de verdad en la carpeta
                if (!nombresEnCarpeta.Contains(csvName))
                {
                    Console.WriteLine($"El archivo {csvName} no existe en la carpeta.");
                    continue;
                }

                //construir la ruta completa y leer el archivo 
                var rutaCompleta = Path.Combine(CsvPath, csvName);
                var lineas = File.ReadAllLines(rutaCompleta, Encoding.UTF8);

                //se comprueba que el CSV no este vacio
                if (lineas.Length == 0)
                {
                    Console.WriteLine("CSV vacío. Se omite.");
                    continue;
                }


                // Detectar separador y leer cabecera
                //   var delimiter = DetectDelimiter(lineas[0]);
                  var delimiter = ',';
                  var headers = SplitCsvLine(lineas[0], delimiter);

                //se comprueba que no este vacio
                if (headers.Count == 0)
                {
                    Console.WriteLine("Cabecera vacía. Se omite.");
                    continue;
                }

                // Determinar esquema y nombre de tabla
                var (schema, tableName) = ParseSchemaAndTable(tabla);


                // Crear tabla si no existe (NVARCHAR(MAX) por columna)
                EnsureTableExists(conn, schema, tableName, headers);

                // Construir DataTable en memoria
                var dataTable = BuildDataTable(headers);


                //recorre las lineas del csv y carga las filas en un DataTabke asegurando que el numero de valores 
                //coincida con el numero de columnas
                for (int r = 1; r < lineas.Length; r++)
                {
                    var values = SplitCsvLine(lineas[r], delimiter);

                    // Alinear longitud (rellenar/truncar)
                    //si hay menos valores que columnas , rellena con null hasta alcanzar ek numero de columnas
                    //si hay mas valores que columnas , trunca la lista a las primeras headers.count columnas
                    if (values.Count < headers.Count)
                        while (values.Count < headers.Count) values.Add(null);
                    else if (values.Count > headers.Count)
                        values = values.Take(headers.Count).ToList();

                    //creacion de la fila DateTable
                    var row = dataTable.NewRow();
                    for (int i = 0; i < headers.Count; i++)
                        row[i] = values[i] ?? (object)DBNull.Value;

                    //añade la fila en el DateTable
                    dataTable.Rows.Add(row);
                }

                // Bulk copy (rápido)
                //se usa sqlBulkerCopy para insertar muchas filas de froma rapida en SQL Server desde
                // un DateTable 
                //crea un objeto sqlBulker
                //el using asegura que se liberen los recursos al finalizar

                //imprime sus columnas
                //foreach (DataColumn col in dataTable.Columns)
                //{
                //    Console.WriteLine(col.ColumnName);
                //}
                using var bulk = new SqlBulkCopy(conn)
                {
                    //especifica la tabla destino
                    DestinationTableName = FullyQualifiedName(schema, tableName),
                    //Define cuantas filas se envian por lote en cada operacion
                    BatchSize = 5000,
                    //establece que la operacion no tenga limite de tiempo
                    BulkCopyTimeout = 0 
                };

                //configura el mapeo de columnas entr el origen (DataTable) y el destino (tabla en SQL Server)
                foreach (var h in headers)
                    bulk.ColumnMappings.Add(h, h);

                //ejecuta la carga maxima: envia todas las filas del DataTable a SQL Server
                //es mas rapido que insertar fila a fila con INSERT 
                bulk.WriteToServer(dataTable);
                //Muestra cuantas filas que se insertaron
                Console.WriteLine($"Filas insertadas: {dataTable.Rows.Count}");
            }
            Console.WriteLine("\nMigración finalizada.");
        }

        //===== Utilidades =====
        
        //recibe un string con el nombre de la tabla y devuelve una tupla con el esquema y el nombre de la tabla
        static (string schema, string table) ParseSchemaAndTable(string tabla)
        {
            //divide el texto por el punto, elimina entradas vacias y espacios
            var parts = tabla.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            // si obtiene dos partes devuelve la tupla con esquema y tabla
            if (parts.Length == 2) return (parts[0], parts[1]);
            return ("dbo", tabla); // por defecto dbo
        }

        //construye el nombre completo de la tabla con esquema y tabla entre corchetes
        static string FullyQualifiedName(string schema, string table)
            => $"{QuoteIdentifier(schema)}.{QuoteIdentifier(table)}";

        //metodo que pasa por parametro la conexion, el esquema, la tabla y las cabeceras
        static void EnsureTableExists(SqlConnection conn, string schema, string table, List<string> headers)
        {
            // Si no existe, crear con NVARCHAR(MAX) por columna
            var cols = string.Join(", ", headers.Select(h => $"{QuoteIdentifier(h)} NVARCHAR(MAX) NULL"));

            //crea un script para crear el esquema si no existe y la tabla si no existe
            var sql = $@" IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{schema}')
                EXEC('CREATE SCHEMA {QuoteIdentifier(schema)}');

IF OBJECT_ID(N'{schema}.{table}', N'U') IS NULL
    EXEC('CREATE TABLE {QuoteIdentifier(schema)}.{QuoteIdentifier(table)} ({cols})');";

            //creaun objeto sqlCommand para ejecutar el script
            using var cmd = conn.CreateCommand();
            //asigna al comando el texto SQL que queires ejecutar 
            cmd.CommandText = sql;
            //ejecuta el comando en la base de datos 
            cmd.ExecuteNonQuery();
        }

        //metodo que pasa por parametro una lista de cabeceras
        static DataTable BuildDataTable(List<string> headers)
        {
            //crea un DataTable() vacio
            var dt = new DataTable();
            //recorre la lista
            foreach (var h in headers)
                //agrega nombre y tipo
                dt.Columns.Add(new DataColumn(h, typeof(string))); // strings → NVARCHAR(MAX)
            //devuelve el DataTable
            return dt;
        }


        //metodo que cuanta si hay mas , o ; y segun el mas devuelve eso como delimitador 
        //static char DetectDelimiter(string line)
        //{
        //    int commas = line.Count(c => c == ',');
        //    int semis = line.Count(c => c == ';');
        //    return semis > commas ? ';' : ',';
        //}

        // Parser sencillo con soporte de comillas dobles y "" como escape
        //este metodo parsea una linea de CSV respetando comillas y demilitadore devolviendo una lista con los campos que contiene 
        static List<string> SplitCsvLine(string line, char delimiter)
        {
            //crea una lista 
            var result = new List<string>();
            //crea un bool
            bool inQuotes = false;
            //es un acumulador de caracteres para contruir el contenido del campoa actual, es mas eficiente que concatenar strings
            //se usa para operaciones de append
            var sb = new StringBuilder();


            for (int i = 0; i < line.Length; i++)
            {
                //obtiene el caracter actual de la posicion i 
                char c = line[i];

                //identifica si el caracter es una comilla
                if (c == '"')
                {
                    //inQuotes indica que estamos dentro de un campo entre comillas
                    //i + 1 < line.Length verifica que existe un caractersiguiente 
                    //line[i + 1] == '"' comprueba que el siguiente caracter es una comilla
                    if (inQuotes && i + 1 < line.Length && line[i + 1] == '"')
                    {
                        sb.Append('"'); i++; //añade una comilla
                    }
                    else
                    {//cambia el estado de inQuotes
                        inQuotes = !inQuotes;
                    }
                    //esto marca el inicio/fin de un campo entrecomillado
                }

                //si el caracter es el demilitador y no estamos dentro de comillas
                else if (c == delimiter && !inQuotes)
                {
                    //convierte el contenido de StringBuilder a String y lo añade d resultado
                    result.Add(sb.ToString());
                    //limpia el buffer para el siguiente campo
                    sb.Clear();
                }
                else
                {
                    //en cualquier otro caso se añade al campo actual en el StringBuilder
                    sb.Append(c);
                }
            }
            //añade el ultimo campo despues del bucle
            result.Add(sb.ToString());

            // Trim simple/ se le quita los espacios
            for (int i = 0; i < result.Count; i++)
                if (result[i] != null) result[i] = result[i].Trim();

            //se devuelve ek resultado completo
            return result;
        }

        // devuelve el identificador SQL Server (nombreTabla, columna , esquema...)

        static string QuoteIdentifier(string name)
            //si es null lo cambia a vacio si tiene ] lo cambia por ]]
            => $"[{(name ?? "").Replace("]", "]]")}]";


        // Mostrar la configuración en consola
        //  Console.WriteLine("Creación de migración:");

        //Creamos una lista de strings
        // List<String> listaCsv = new List<String>();

        //recorre migrationConfig y va sacando los diferentes valores 
        //foreach (var item in migrationConfig)
        //{
        //    // Console.WriteLine($"ID: {item.Key} | CSV: {item.Value.Csv} | Tabla: {item.Value.Tabla}");

        //    //metemos todos los valores del CSV en la lista
        //    listaCsv.Add(item.Value.Csv);


        //}
        //creamos un array de la ruta de los archivso que contiene la carpeta de los CSV
        //string[] archivosEnCarpeta = Directory.GetFiles(CsvPath);

        //Aqui esta una lista y ya solo coge los nombres de los archivos sin la ruta completa
        //var nombresEnCarpeta = archivosEnCarpeta.Select(f => Path.GetFileName(f)).ToList();


        // string[] nombresEnCarpetaSinEspacios = nombresEnCarpeta.Select(s => s.Trim()).ToArray();
        //recorremos la lista 
        //foreach (var i in listaCsv)
        //{

        //    //comprobamos si es la lista coinciden
        //    if (nombresEnCarpeta.Contains(i))
        //    {
        //        string rutaCompleta = Path.Combine(CsvPath, i);

        //        if (File.Exists(rutaCompleta))
        //        {

        //                String[] arrayCompletoCSV = File.ReadAllLines(rutaCompleta);
        //            foreach (var item in arrayCompletoCSV)
        //            {
        //                Console.WriteLine(item);
        //            }


        //        }
        //        else
        //        {
        //            Console.WriteLine($" El archivo {i} no existe en la carpeta.");
        //        }

        //    }
        //    else
        //    {
        //        Console.WriteLine($" {i} NO está en el array.");
        //    }
        //}

    }
}