# USDA FruitAndVegetables Data Source

QuantConnect data source integration for USDA FruitAndVegetables.

## Overview

USDA ERS retail price estimates for 150+ commonly consumed fresh and processed fruits and vegetables. Includes price per pound/pint and normalized price per edible cup equivalent.

Each produce + form combination is its own data series (symbol). Data is stored per series in `alternative/usda/fruitandvegetables/{series}.csv` with the date in the first column (for example, `alternative/usda/fruitandvegetables/apples_fresh.csv`).


## Data Characteristics

| Property | Value |
|----------|-------|
| **Data Source ID** | 0 |
| **Format** | CSV |
| **Resolution** | Daily |
| **Timezone** | Utc |
| **Requires Mapping** | False |
| **Sparse Data** | True |

## Usage

### C# Example

```csharp
public class MyAlgorithm : QCAlgorithm
{
    public override void Initialize()
    {

        AddData<USDAFruitAndVegetables>(USDAFruitAndVegetables.Apples.Fresh, Resolution.Daily);

    }

    public override void OnData(Slice slice)
    {
        var data = slice.Get<USDAFruitAndVegetables>();
        // Process data...
    }
}
```

### Python Example

```python
class MyAlgorithm(QCAlgorithm):
    def initialize(self):

        self.add_data(USDAFruitAndVegetables, USDAFruitAndVegetables.Apples.Fresh, Resolution.DAILY)


    def on_data(self, slice):
        data = slice.get(USDAFruitAndVegetables)
        # Process data...
```

## Data Processing

The `DataProcessing` workflow downloads (or reads) USDA ERS Fruit & Vegetable Prices `.xlsx` workbooks and emits per-series CSV files in Lean's data folder structure.

Each XLSX workbook from USDA ERS contains pricing data organized by produce type and form (Fresh, Canned, Frozen, Dried, Juice).

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `TEMP_OUTPUT_DIRECTORY` | `/temp-output-directory` | Root output directory for processed data |

Output path: `$TEMP_OUTPUT_DIRECTORY/alternative/usda/fruitandvegetables/{series}.csv`

### Run

```bash
# Run notebook with default output directory (/temp-output-directory)
jupyter nbconvert --to notebook --execute DataProcessing/process.ipynb

# Run with custom output directory for local development
TEMP_OUTPUT_DIRECTORY="./output" jupyter nbconvert --to notebook --execute DataProcessing/process.ipynb
```

### Outputs

- CSV files: `alternative/usda/fruitandvegetables/{series}.csv`
- Symbol constants: `USDAFruitAndVegetables.Symbols.cs` (191 series)

## Building

```bash
dotnet build
```

## Testing

```bash
dotnet test tests/Tests.csproj
```

## License

This library is provided by QuantConnect under the Apache 2.0 license.
