# USDA Fruit and Vegetable Prices Documentation

## Overview

USDA ERS retail price estimates for 150+ commonly consumed fresh and processed fruits and vegetables. Includes price per pound/pint and normalized price per edible cup equivalent.


## Data Properties

| Property | Type | Nullable | Description |
|----------|------|----------|-------------|
| `AverageRetailPrice` | `decimal?` | Yes | Average retail price per unit (pound or pint). |
| `Unit` | `PriceUnit?` | Yes | Unit of measure for `AverageRetailPrice` (`per_pound` or `per_pint`). |
| `PreparationYieldFactor` | `decimal?` | Yes | Fraction of product that is edible after preparation (0.0–1.0). |
| `CupEquivalentSize` | `decimal?` | Yes | Size of one edible cup equivalent. |
| `CupEquivalentUnit` | `CupEquivalentUnit?` | Yes | Unit of measure for `CupEquivalentSize` (`pounds`, `pints`, or `fluid_ounces`). |
| `PricePerCupEquivalent` | `decimal?` | Yes | Normalized price per edible cup equivalent (this is the `Value`). |

**Note**: The `Value` property returns `PricePerCupEquivalent ?? 0m` for chart compatibility. Check `.HasValue` on underlying properties when null-awareness is needed.


## Usage

### Adding the Data

#### C#
```csharp
public class MyAlgorithm : QCAlgorithm
{
    public override void Initialize()
    {

        // Each produce + form combination is its own data series (symbol), for example: Apples.Fresh
        AddData<USDAFruitAndVegetables>(USDAFruitAndVegetables.Apples.Fresh, Resolution.Daily);

    }
}
```

#### Python
```python
class MyAlgorithm(QCAlgorithm):
    def initialize(self):

        # Each produce + form combination is its own data series (symbol), for example: Apples.Fresh
        self.add_data(USDAFruitAndVegetables, USDAFruitAndVegetables.Apples.Fresh, Resolution.DAILY)

```

### Accessing the Data

#### C#
```csharp
public override void OnData(Slice slice)
{
    var data = slice.Get<USDAFruitAndVegetables>();
    foreach (var kvp in data)
    {
        var symbol = kvp.Key;
        var point = kvp.Value;

        Log($"{symbol}: {point}");

    }
}
```

#### Python
```python
def on_data(self, slice):
    data = slice.get(USDAFruitAndVegetables)
    for symbol, point in data.items():

        self.log(f"{symbol}: {point}")

```



## Data Source Details

| Attribute | Value |
|-----------|-------|
| Data Source ID | 0 |
| Resolution | Daily |
| Timezone | Utc |
| Requires Mapping | False |
| Sparse Data | True |

## Data Coverage

- **Start Date**: 2013-01-01
- **End Date**: 2023-01-01
- **Asset Coverage**: 191 series (produce + form combinations)
- **Data Density**: ~4 rows/series (annual data points)
- **Data Process Duration**: 25.91s
- **Update Process Duration**: TBD

## Data Processing

The processor generates per-series CSV files under `alternative/usda/fruitandvegetables/` (for example, `apples_fresh.csv`).

**CSV Row Format:**
```
yyyyMMdd,AverageRetailPrice,Unit,PreparationYieldFactor,CupEquivalentSize,CupEquivalentUnit,PricePerCupEquivalent
```

See the repository README for configuration options and processing instructions.
