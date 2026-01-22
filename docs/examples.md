# USDA FruitAndVegetables Examples

## Quick Reference

### Available Symbols

Symbols are accessed via `USDAFruitAndVegetable.Symbols.<Product>`:

```csharp
// C# - Common examples
USDAFruitAndVegetable.Symbols.Apples       // Includes Fresh, Applesauce, Juice variants
USDAFruitAndVegetable.Symbols.Bananas      // Fresh
USDAFruitAndVegetable.Symbols.Strawberries // Fresh, Frozen
USDAFruitAndVegetable.Symbols.Broccoli     // Fresh, Frozen
USDAFruitAndVegetable.Symbols.Oranges      // Fresh, Juice
```

```python
# Python - Common examples
USDAFruitAndVegetable.Symbols.Apples       # Includes Fresh, Applesauce, Juice variants
USDAFruitAndVegetable.Symbols.Bananas      # Fresh
USDAFruitAndVegetable.Symbols.Strawberries # Fresh, Frozen
USDAFruitAndVegetable.Symbols.Broccoli     # Fresh, Frozen
USDAFruitAndVegetable.Symbols.Oranges      # Fresh, Juice
```

For the complete list of 75 products, browse `USDAFruitAndVegetable.Symbols.cs`.

### Class Structure

- **`USDAFruitAndVegetables`** (plural): Collection class for subscription
- **`USDAFruitAndVegetable`** (singular): Individual data point with `Form` property

### Data Properties

Each data point provides:

| Property | Type | Description |
|----------|------|-------------|
| `Form` | string | Product form (Fresh, Canned, Frozen, Juice, etc.) |
| `Value` | decimal | Price per cup equivalent (normalized), returns 0 if null |
| `AverageRetailPrice` | decimal? | Original retail price |
| `Unit` | PriceUnit? | Original price unit (PerPound, PerPint) |
| `PreparationYieldFactor` | decimal? | Edible portion after prep (0.0–1.0) |
| `CupEquivalentSize` | decimal? | Cup equivalent portion size |
| `CupEquivalentUnit` | CupEquivalentUnit? | Unit for cup equivalent (Pounds, Pints, FluidOunces) |
| `PricePerCupEquivalent` | decimal? | Normalized price per cup (this backs `Value`) |

## Basic Usage Example

### C# Algorithm

```csharp
using QuantConnect.Data;
using QuantConnect.DataSource;

namespace QuantConnect.Algorithm.CSharp
{
    public class USDAFruitAndVegetablesExampleAlgorithm : QCAlgorithm
    {
        private Symbol _dataSymbol;

        public override void Initialize()
        {
            SetStartDate(2020, 1, 1);
            SetEndDate(2020, 12, 31);
            SetCash(100000);

            // One subscription gets all Apple forms (Fresh, Applesauce, Juice, etc.)
            _dataSymbol = AddData<USDAFruitAndVegetables>(
                USDAFruitAndVegetable.Symbols.Apples, Resolution.Daily).Symbol;
        }

        public override void OnData(Slice slice)
        {
            foreach (var kvp in slice.Get<USDAFruitAndVegetables>())
            {
                var collection = kvp.Value;

                // Iterate over all forms in the collection
                foreach (USDAFruitAndVegetable data in collection.Data)
                {
                    Log($"{Time}: {kvp.Key} [{data.Form}] - ${data.PricePerCupEquivalent}");
                }
            }
        }
    }
}
```

### Python Algorithm

```python
from AlgorithmImports import *

class USDAFruitAndVegetablesExampleAlgorithm(QCAlgorithm):

    def initialize(self):
        self.set_start_date(2020, 1, 1)
        self.set_end_date(2020, 12, 31)
        self.set_cash(100000)

        # One subscription gets all Apple forms (Fresh, Applesauce, Juice, etc.)
        self.data_symbol = self.add_data(
            USDAFruitAndVegetables,
            USDAFruitAndVegetable.Symbols.Apples,
            Resolution.DAILY
        ).symbol

    def on_data(self, slice):
        for symbol, collection in slice.get(USDAFruitAndVegetables).items():
            # Iterate over all forms in the collection
            for data in collection.data:
                self.log(f"{self.time}: {symbol} [{data.form}] - ${data.price_per_cup_equivalent}")
```

## Comparing Multiple Products

Track price trends across different products. Each subscription now includes all forms:

### Python

```python
from AlgorithmImports import *

class ProduceComparisonAlgorithm(QCAlgorithm):

    def initialize(self):
        self.set_start_date(2018, 1, 1)
        self.set_end_date(2023, 12, 31)

        # Subscribe to multiple products - each includes all forms
        self.symbols = [
            self.add_data(USDAFruitAndVegetables, USDAFruitAndVegetable.Symbols.Apples).symbol,
            self.add_data(USDAFruitAndVegetables, USDAFruitAndVegetable.Symbols.Oranges).symbol,
            self.add_data(USDAFruitAndVegetables, USDAFruitAndVegetable.Symbols.Bananas).symbol,
        ]

    def on_data(self, slice):
        for symbol, collection in slice.get(USDAFruitAndVegetables).items():
            # Filter to Fresh forms only for comparison
            for data in collection.data:
                if data.form == "Fresh":
                    self.log(f"{self.time}: {symbol} Fresh - ${data.price_per_cup_equivalent:.2f}/cup")
```

## Understanding Sparse Data

USDA pricing data is **annual** - you'll receive one data point per year per form. Each collection may contain multiple forms on the same date:

### Python

```python
from AlgorithmImports import *

class SparseDataAlgorithm(QCAlgorithm):

    def initialize(self):
        self.set_start_date(2018, 1, 1)
        self.set_end_date(2023, 12, 31)

        self.produce = self.add_data(
            USDAFruitAndVegetables,
            USDAFruitAndVegetable.Symbols.Strawberries
        ).symbol

        self.last_data_time = None

    def on_data(self, slice):
        if slice.contains_key(self.produce):
            collection = slice[self.produce]

            if self.last_data_time:
                days_since = (self.time - self.last_data_time).days
                self.log(f"Received data after {days_since} days gap")

            self.last_data_time = self.time

            # Log all forms received
            for data in collection.data:
                self.log(f"{self.time.year}: Strawberries ({data.form}) ${data.average_retail_price:.2f}")
```

**Key point**: Data points arrive once per year. For daily trading decisions, consider using this data as a reference signal rather than a real-time feed.

## Accessing Data Properties

### Python

```python
from AlgorithmImports import *

class DataPropertiesAlgorithm(QCAlgorithm):

    def initialize(self):
        self.set_start_date(2020, 1, 1)
        self.set_end_date(2023, 12, 31)

        self.broccoli = self.add_data(
            USDAFruitAndVegetables,
            USDAFruitAndVegetable.Symbols.Broccoli
        ).symbol

    def on_data(self, slice):
        if slice.contains_key(self.broccoli):
            collection = slice[self.broccoli]

            for data in collection.data:
                self.log(f"--- {self.time.year} Broccoli ({data.form}) ---")
                self.log(f"  Retail price: ${data.average_retail_price:.2f} {data.unit}")
                self.log(f"  Prep yield: {data.preparation_yield_factor:.1%} edible after prep")
                self.log(f"  Cup equivalent: {data.cup_equivalent_size} {data.cup_equivalent_unit}")
                self.log(f"  Price per cup: ${data.value:.2f} (normalized)")
```

## Fresh vs Frozen Comparison

Compare costs between fresh and frozen forms **using a single subscription**:

### Python

```python
from AlgorithmImports import *

class FreshVsFrozenAlgorithm(QCAlgorithm):

    def initialize(self):
        self.set_start_date(2018, 1, 1)
        self.set_end_date(2023, 12, 31)

        # Single subscription gets both Fresh and Frozen forms
        self.strawberries = self.add_data(
            USDAFruitAndVegetables,
            USDAFruitAndVegetable.Symbols.Strawberries
        ).symbol

    def on_data(self, slice):
        if slice.contains_key(self.strawberries):
            collection = slice[self.strawberries]

            # Find Fresh and Frozen forms in the collection
            fresh_price = None
            frozen_price = None

            for data in collection.data:
                if data.form == "Fresh":
                    fresh_price = data.value
                elif data.form == "Frozen":
                    frozen_price = data.value

            if fresh_price and frozen_price:
                diff_pct = ((fresh_price - frozen_price) / frozen_price) * 100
                self.log(f"{self.time.year}: Fresh ${fresh_price:.2f} vs Frozen ${frozen_price:.2f} ({diff_pct:+.1f}%)")
```

## Filtering by Form

Use LINQ (C#) or list comprehensions (Python) to filter the collection:

### C#

```csharp
// Get only Fresh forms
var freshData = collection.Data
    .Cast<USDAFruitAndVegetable>()
    .Where(d => d.Form == "Fresh");

// Get all Juice forms (including "Juice, Ready to drink", "Juice Frozen", etc.)
var juiceData = collection.Data
    .Cast<USDAFruitAndVegetable>()
    .Where(d => d.Form.Contains("Juice"));
```

### Python

```python
# Get only Fresh forms
fresh_data = [d for d in collection.data if d.form == "Fresh"]

# Get all Juice forms
juice_data = [d for d in collection.data if "Juice" in d.form]
```
