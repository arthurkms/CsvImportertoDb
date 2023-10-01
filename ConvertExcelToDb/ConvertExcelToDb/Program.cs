using System.Globalization;
using Npgsql;
using CsvHelper;
using CsvHelper.Configuration;

string csvFilePath = "C:\\Users\\arthu\\OneDrive\\Desktop\\teste.csv";
string connectionString = "Host=localhost;Port=37010;Username=postgres;Password=502151;Database=postgres";

try
{
    using (var reader = new StreamReader(csvFilePath))
    using (var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)))
    {
        var records = csv.GetRecords<dynamic>().ToList(); // Read CSV as dynamic objects

        if (records.Count == 0)
        {
            Console.WriteLine("CSV file is empty.");
            return;
        }

        using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync();

            var header = (IDictionary<string, object>)records[0]; // Assuming the first row is the header

            for (int i = 1; i < records.Count; i++)
            {
                var data = (IDictionary<string, object>)records[i];

                // Generate the dynamic SQL insert query
                string tableName = "kenzo.fakedata"; // Replace with your table name
                string columns = string.Join(", ", header.Keys);
                string values = string.Join(", ", header.Keys.Select(key => $"'{data[key]}'"));

                string insertQuery = $"INSERT INTO {tableName} ({columns}) VALUES ({values})";

                using (var cmd = new NpgsqlCommand(insertQuery, connection))
                {
                    await cmd.ExecuteNonQueryAsync();
                }
            }

            Console.WriteLine("Data inserted successfully.");
        }
    }
}
catch (Exception ex)
{
    Console.WriteLine($"Error: {ex.Message}");
}

