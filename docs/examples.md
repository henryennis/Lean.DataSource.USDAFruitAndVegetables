# USDA FruitAndVegetables Examples

## Quick Reference

### Available Symbols

Symbols follow the pattern `USDAFruitAndVegetables.<Produce>.<Form>`:

```csharp
// C# - Common examples
USDAFruitAndVegetables.Apples.Fresh
USDAFruitAndVegetables.Apples.Juice
USDAFruitAndVegetables.Bananas.Fresh
USDAFruitAndVegetables.Strawberries.Fresh
USDAFruitAndVegetables.Strawberries.Frozen
USDAFruitAndVegetables.Broccoli.Fresh
USDAFruitAndVegetables.Broccoli.Frozen
```

```python
# Python - Common examples
USDAFruitAndVegetables.Apples.Fresh
USDAFruitAndVegetables.Apples.Juice
USDAFruitAndVegetables.Bananas.Fresh
USDAFruitAndVegetables.Strawberries.Fresh
USDAFruitAndVegetables.Strawberries.Frozen
USDAFruitAndVegetables.Broccoli.Fresh
USDAFruitAndVegetables.Broccoli.Frozen
```

For the complete list of 191 series, browse the `USDAFruitAndVegetables.Symbols` class.

### Data Properties

Each data point provides:

| Property | Type | Description |
|----------|------|-------------|
| `Value` | `decimal` | Price per cup equivalent (normalized, never null) |
| `AverageRetailPrice` | `decimal?` | Original retail price |
| `Unit` | `PriceUnit?` | Original price unit (`PerPound`, `PerPint`) |
| `PreparationYieldFactor` | `decimal?` | Edible portion after prep (0.0-1.0) |
| `CupEquivalentSize` | `decimal?` | Cup equivalent portion size |
| `CupEquivalentUnit` | `CupEquivalentUnit?` | Unit for cup equivalent (`Pounds`, `Pints`, `FluidOunces`) |

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


            _dataSymbol = AddData<USDAFruitAndVegetables>(USDAFruitAndVegetables.Apples.Fresh, Resolution.Daily).Symbol;

        }

        public override void OnData(Slice slice)
        {
            var data = slice.Get<USDAFruitAndVegetables>();

            foreach (var kvp in data)
            {

                // Process data point
                Log($"{Time}: {kvp.Key} - {kvp.Value}");

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


        self.data_symbol = self.add_data(USDAFruitAndVegetables, USDAFruitAndVegetables.Apples.Fresh, Resolution.DAILY).symbol


    def on_data(self, slice):
        data = slice.get(USDAFruitAndVegetables)

        for symbol, point in data.items():

            # Process data point
            self.log(f"{self.time}: {symbol} - {point}")

```

## Comparing Multiple Produce Types

Track price trends across different produce categories:

### Python

```python
from AlgorithmImports import *

class ProduceComparisonAlgorithm(QCAlgorithm):

    def initialize(self):
        self.set_start_date(2018, 1, 1)
        self.set_end_date(2023, 12, 31)

        # Subscribe to multiple produce types
        self.symbols = [
            self.add_data(USDAFruitAndVegetables, USDAFruitAndVegetables.Apples.Fresh).symbol,
            self.add_data(USDAFruitAndVegetables, USDAFruitAndVegetables.Oranges.Fresh).symbol,
            self.add_data(USDAFruitAndVegetables, USDAFruitAndVegetables.Bananas.Fresh).symbol,
        ]

        self.prices = {}

    def on_data(self, slice):
        for symbol in self.symbols:
            if slice.contains_key(symbol):
                point = slice[symbol]
                # Use normalized price per cup equivalent for comparison
                self.prices[symbol] = point.value

        if len(self.prices) == len(self.symbols):
            cheapest = min(self.prices, key=self.prices.get)
            self.log(f"{self.time}: Cheapest fruit: {cheapest} at ${self.prices[cheapest]:.2f}/cup")
```

## Understanding Sparse Data

USDA pricing data is **annual** - you'll receive one data point per year per series. This affects how you use the data in backtests:

### Python

```python
from AlgorithmImports import *

class SparseDataAlgorithm(QCAlgorithm):

    def initialize(self):
        self.set_start_date(2018, 1, 1)
        self.set_end_date(2023, 12, 31)

        self.produce = self.add_data(
            USDAFruitAndVegetables,
            USDAFruitAndVegetables.Strawberries.Fresh
        ).symbol

        # Track when we last received data
        self.last_data_time = None

    def on_data(self, slice):
        if slice.contains_key(self.produce):
            point = slice[self.produce]

            if self.last_data_time:
                days_since = (self.time - self.last_data_time).days
                self.log(f"Received data after {days_since} days gap")

            self.last_data_time = self.time
            self.log(f"{self.time.year}: Strawberries ${point.average_retail_price:.2f}/{point.unit}")
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
            USDAFruitAndVegetables.Broccoli.Fresh
        ).symbol

    def on_data(self, slice):
        if slice.contains_key(self.broccoli):
            point = slice[self.broccoli]

            self.log(f"--- {self.time.year} Broccoli (Fresh) ---")
            self.log(f"  Retail price: ${point.average_retail_price:.2f} {point.unit}")
            self.log(f"  Prep yield: {point.preparation_yield_factor:.1%} edible after prep")
            self.log(f"  Cup equivalent: {point.cup_equivalent_size} {point.cup_equivalent_unit}")
            self.log(f"  Price per cup: ${point.value:.2f} (normalized)")
```

## Fresh vs Frozen Comparison

Compare costs between fresh and processed forms:

### Python

```python
from AlgorithmImports import *

class FreshVsFrozenAlgorithm(QCAlgorithm):

    def initialize(self):
        self.set_start_date(2018, 1, 1)
        self.set_end_date(2023, 12, 31)

        self.fresh = self.add_data(
            USDAFruitAndVegetables,
            USDAFruitAndVegetables.Strawberries.Fresh
        ).symbol

        self.frozen = self.add_data(
            USDAFruitAndVegetables,
            USDAFruitAndVegetables.Strawberries.Frozen
        ).symbol

    def on_data(self, slice):
        fresh_price = None
        frozen_price = None

        if slice.contains_key(self.fresh):
            fresh_price = slice[self.fresh].value
        if slice.contains_key(self.frozen):
            frozen_price = slice[self.frozen].value

        if fresh_price and frozen_price:
            diff_pct = ((fresh_price - frozen_price) / frozen_price) * 100
            self.log(f"{self.time.year}: Fresh ${fresh_price:.2f} vs Frozen ${frozen_price:.2f} ({diff_pct:+.1f}%)")
```
