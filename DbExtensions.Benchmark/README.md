# DbExtensions.Benchmark

Compare Dapper row-by-row writes with ETLBox.DbExtensions bulk operations (and SqlBulkCopy for inserts) on SQL Server.

```bash
dotnet run -c Release
dotnet run -c Release -- --rows 5000 --cs "Data Source=localhost;User Id=sa;Password=...;Initial Catalog=demo;TrustServerCertificate=true;"
```

- `--rows` defaults to 5000. Stay at or below 4999 without a license key; copy `etlbox.lic` next to this project for larger sets.
- Connection string: `--cs`, or env `DBEXTENSIONS_BENCHMARK_CS`, or the localhost default used by the other DbExtensions demos.
- `--skip-loop` skips the Dapper foreach baselines when the row count would make them too slow.

The console prints a markdown table at the end for pasting into the DbExtensions docs.
