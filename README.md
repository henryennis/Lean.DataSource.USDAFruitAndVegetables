# USDA FruitAndVegetables Data Source

QuantConnect data source integration for USDA FruitAndVegetables.

## Overview

USDA ERS retail price estimates for 150+ commonly consumed fresh and processed fruits and vegetables. Includes price per pound/pint and normalized price per edible cup equivalent.

Each product contains all forms (Fresh, Canned, Frozen, Juice, etc.) as rows in a single file. Data is stored per product in `alternative/usda/fruitandvegetables/{product}.csv` (for example, `alternative/usda/fruitandvegetables/apples.csv`).


## Data Characteristics

| Property | Value |
|----------|-------|
| **Data Source ID** | 0 |
| **Format** | CSV |
| **Resolution** | Daily |
| **Timezone** | Utc |
| **Requires Mapping** | False |
| **Sparse Data** | True |
| **Data Model** | Collection (BaseDataCollection) |

### Class Structure

- **`USDAFruitAndVegetable`** (singular): Individual data point with `Form` property
- **`USDAFruitAndVegetables`** (plural): Collection class for subscription

### Nullable Fields

Source data may have missing measurements. The following fields are nullable:
- `AverageRetailPrice` (`decimal?`)
- `PreparationYieldFactor` (`decimal?`)
- `CupEquivalentSize` (`decimal?`)
- `PricePerCupEquivalent` (`decimal?`)
- `Unit` (`PriceUnit?`)
- `CupEquivalentUnit` (`CupEquivalentUnit?`)

The `Value` property returns `PricePerCupEquivalent ?? 0m` for chart compatibility. Check `.HasValue` on underlying properties when null-awareness is needed.

## Usage

### C# Example

```csharp
public class MyAlgorithm : QCAlgorithm
{
    public override void Initialize()
    {
        // One subscription for all Apple forms (Fresh, Applesauce, Juice, etc.)
        AddData<USDAFruitAndVegetables>(USDAFruitAndVegetable.Symbols.Apples, Resolution.Daily);
    }

    public override void OnData(Slice slice)
    {
        foreach (var kvp in slice.Get<USDAFruitAndVegetables>())
        {
            var collection = kvp.Value;
            foreach (USDAFruitAndVegetable data in collection.Data)
            {
                Log($"{data.Form}: ${data.PricePerCupEquivalent}");
            }
        }
    }
}
```

### Python Example

```python
class MyAlgorithm(QCAlgorithm):
    def initialize(self):
        # One subscription for all Apple forms (Fresh, Applesauce, Juice, etc.)
        self.add_data(USDAFruitAndVegetables, USDAFruitAndVegetable.Symbols.Apples, Resolution.DAILY)

    def on_data(self, slice):
        for symbol, collection in slice.get(USDAFruitAndVegetables).items():
            for data in collection.data:
                self.log(f"{data.form}: ${data.price_per_cup_equivalent}")
```

### Filtering by Form

```csharp
// Get only Fresh form data
var freshData = collection.Data
    .Cast<USDAFruitAndVegetable>()
    .Where(d => d.Form == "Fresh");
```

## Data Processing

The `process.ipynb` notebook downloads (or reads) USDA ERS Fruit & Vegetable Prices `.xlsx` workbooks and emits per-product CSV files in Lean's data folder structure.

Each XLSX workbook from USDA ERS contains pricing data organized by produce type. All forms for a product (Fresh, Canned, Frozen, Dried, Juice) are consolidated into a single output file.

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `TEMP_OUTPUT_DIRECTORY` | `/temp-output-directory` | Root output directory for processed data |

Output path: `$TEMP_OUTPUT_DIRECTORY/alternative/usda/fruitandvegetables/{product}.csv`

### Run

```bash
# Run notebook with default output directory (/temp-output-directory)
jupyter nbconvert --to notebook --execute process.ipynb

# Run with custom output directory for local development
TEMP_OUTPUT_DIRECTORY="./output" jupyter nbconvert --to notebook --execute process.ipynb
```

### Outputs

- CSV files: `alternative/usda/fruitandvegetables/{product}.csv` (75 files)
- Helper class: `USDAFruitAndVegetable.Symbols.cs` (contains product symbols)

## Building

```bash
dotnet build
```

## Testing

```bash
dotnet test tests/Tests.csproj
```

C# tests validate the data contract at the consumption boundary.

### Integration Tests

Integration tests validate parsing against actual sample data files. These require processed CSV files in the output directory.

```bash
# First, ensure sample data exists
make process

# Run all tests including integration tests
dotnet test tests/Tests.csproj

# Run only integration tests
dotnet test tests/Tests.csproj --filter "FullyQualifiedName~IntegrationTests"
```

Integration tests gracefully skip (not fail) if `output/` contains no sample data.

**What integration tests validate:**
- Sample file parsing: All rows parse without exceptions
- Schema contract: CSV files have exactly 8 columns with valid date format
- Form consistency: Detects naming variations across years (e.g., "Juice - Frozen" vs "Juice; frozen")
- Data coverage: Reports which years have data for each product
- GetSource alignment: Verifies `Symbols` class entries match actual CSV files

## License

This library is provided by QuantConnect under the Apache 2.0 license.
