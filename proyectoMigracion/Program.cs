using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.IO;
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
            // Ruta del archivo JSON
            string jsonPath = "migration.json";

            // Ruta de los archivos CSV
            string CsvPath = "C:\\Users\\sergio.paredes-beca\\source\\repos\\proyectoMigracion\\proyectoMigracion\\bin\\Debug\\net8.0\\archivosCSV";


            //comprueba si existe o no el archivo 
            if (!File.Exists(jsonPath))
            {
                Console.WriteLine("El archivo migration.json no existe.");
                return;
            }

            // Leer el contenido del JSON
            string jsonContent = File.ReadAllText(jsonPath);

            // Deserializar a un diccionario
            //ignora mayusculas y minusculas
            var option = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
            //convierte el contenido de un JSON a un dicionario 
            var migrationConfig = JsonSerializer.Deserialize<Dictionary<string, MigrationItem>>(jsonContent, option);

            //si esta vacio o ni tiene nada 
            if (migrationConfig == null || migrationConfig.Count == 0)
            {

                Console.WriteLine("No se encontraron configuraciones en el JSON.");
                return;
            }

            // Mostrar la configuración en consola
            Console.WriteLine("Creación de migración:");

            //Creamos una lista de strings
            List<String> listaCsv = new List<String>();

            //recorre migrationConfig y va sacando los diferentes valores 
            foreach (var item in migrationConfig)
            {
                // Console.WriteLine($"ID: {item.Key} | CSV: {item.Value.Csv} | Tabla: {item.Value.Tabla}");

                //metemos todos los valores del CSV en la lista
                listaCsv.Add(item.Value.Csv);


            }
            //creamos un array de la ruta de los archivso que contiene la carpeta de los CSV
            string[] archivosEnCarpeta = Directory.GetFiles(CsvPath);

            //Aqui esta una lista y ya solo coge los nombres de los archivos sin la ruta completa
            var nombresEnCarpeta = archivosEnCarpeta.Select(f => Path.GetFileName(f)).ToList();
            

            // string[] nombresEnCarpetaSinEspacios = nombresEnCarpeta.Select(s => s.Trim()).ToArray();
            //recorremos la lista 
            foreach (var i in listaCsv)
            {

                //comprobamos si es la lista coinciden
                if (nombresEnCarpeta.Contains(i))
                {
                    string rutaCompleta = Path.Combine(CsvPath, i);

                    if (File.Exists(rutaCompleta))
                    {
                       
                            String[] arrayCompletoCSV = File.ReadAllLines(rutaCompleta);

                      
                    }else
                    {
                        Console.WriteLine($" El archivo {i} no existe en la carpeta.");
                    }

                }
                else
                {
                    Console.WriteLine($" {i} NO está en el array.");

                }
            }


        }
    }
}