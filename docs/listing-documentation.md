# USDA Fruit and Vegetable Prices Documentation

## Overview

USDA ERS retail price estimates for 150+ commonly consumed fresh and processed fruits and vegetables. Includes price per pound/pint and normalized price per edible cup equivalent.

Each product subscription includes all forms (Fresh, Canned, Frozen, Juice, etc.) as a collection.

## Class Structure

- **`USDAFruitAndVegetables`** (plural): Collection class for subscription
- **`USDAFruitAndVegetable`** (singular): Individual data point with `Form` property

## Data Properties

| Property | Type | Description |
|----------|------|-------------|
| `Form` | `string` | Product form identifier (e.g., "Fresh", "Canned", "Fresh - Florets"). |
| `AverageRetailPrice` | `decimal?` | Average retail price per unit (pound or pint). |
| `Unit` | `PriceUnit?` | Unit of measure for `AverageRetailPrice` (`per_pound` or `per_pint`). |
| `PreparationYieldFactor` | `decimal?` | Fraction of product that is edible after preparation (0.0–1.0). |
| `CupEquivalentSize` | `decimal?` | Size of one edible cup equivalent. |
| `CupEquivalentUnit` | `CupEquivalentUnit?` | Unit of measure for `CupEquivalentSize` (`pounds`, `pints`, or `fluid_ounces`). |
| `PricePerCupEquivalent` | `decimal?` | Normalized price per edible cup equivalent (this is the `Value`). |

**Note**: The `Value` property returns `PricePerCupEquivalent ?? 0m` for chart compatibility. Check `.HasValue` on underlying properties when null-awareness is needed.


## Usage

### Adding the Data

#### C#
```csharp
public class MyAlgorithm : QCAlgorithm
{
    public override void Initialize()
    {
        // One subscription per product - includes all forms (Fresh, Canned, Juice, etc.)
        AddData<USDAFruitAndVegetables>(USDAFruitAndVegetable.Symbols.Apples, Resolution.Daily);
    }
}
```

#### Python
```python
class MyAlgorithm(QCAlgorithm):
    def initialize(self):
        # One subscription per product - includes all forms (Fresh, Canned, Juice, etc.)
        self.add_data(USDAFruitAndVegetables, USDAFruitAndVegetable.Symbols.Apples, Resolution.DAILY)
```

### Accessing the Data

#### C#
```csharp
public override void OnData(Slice slice)
{
    foreach (var kvp in slice.Get<USDAFruitAndVegetables>())
    {
        var symbol = kvp.Key;
        var collection = kvp.Value;

        // Iterate over all forms in the collection
        foreach (USDAFruitAndVegetable data in collection.Data)
        {
            Log($"{symbol} [{data.Form}]: ${data.PricePerCupEquivalent}");
        }
    }
}
```

#### Python
```python
def on_data(self, slice):
    for symbol, collection in slice.get(USDAFruitAndVegetables).items():
        # Iterate over all forms in the collection
        for data in collection.data:
            self.log(f"{symbol} [{data.form}]: ${data.price_per_cup_equivalent}")
```

### Filtering by Form

#### C#
```csharp
// Get only Fresh form data
var freshData = collection.Data
    .Cast<USDAFruitAndVegetable>()
    .Where(d => d.Form == "Fresh");
```

#### Python
```python
# Get only Fresh form data
fresh_data = [d for d in collection.data if d.form == "Fresh"]
```


## Data Source Details

| Attribute | Value |
|-----------|-------|
| Data Source ID | 0 |
| Data Model | Collection (BaseDataCollection) |
| Resolution | Daily |
| Timezone | Utc |
| Requires Mapping | False |
| Sparse Data | True |

## Data Coverage

- **Start Date**: 2013-01-01
- **End Date**: 2023-01-01
- **Product Coverage**: 75 products (each containing multiple forms)
- **Data Density**: Multiple forms per product per year
- **Data Process Duration**: ~25s

## Data Processing

The processor generates per-product CSV files under `alternative/usda/fruitandvegetables/` (for example, `apples.csv`).

**CSV Row Format (8 columns):**
```
yyyyMMdd,Form,AverageRetailPrice,Unit,PreparationYieldFactor,CupEquivalentSize,CupEquivalentUnit,PricePerCupEquivalent
```

**Example:**
```
20200101,Fresh,1.52,per_pound,0.9,0.25,pounds,0.50
20200101,Applesauce,1.08,per_pound,1.0,0.30,pounds,0.45
20200101,Juice Frozen,2.10,per_pint,1.0,0.50,pints,0.80
```

See the repository README for configuration options and processing instructions.
