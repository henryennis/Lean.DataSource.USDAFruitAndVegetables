# QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
# Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from AlgorithmImports import *


class USDAFruitAndVegetablesAlgorithm(QCAlgorithm):
    """Example algorithm using USDAFruitAndVegetables data.

    Demonstrates the collection pattern where one subscription provides all product forms.
    Covers: multi-product subscription, sparse data handling, full property access,
    Fresh vs Frozen comparison, and list comprehension filtering.
    """

    def initialize(self) -> None:
        """Initialize the algorithm"""
        self.set_start_date(2018, 1, 1)
        self.set_end_date(2023, 12, 31)
        self.set_cash(100000)

        # Multi-product subscription - each includes all forms for that product
        self.apple_symbol = self.add_data(
            USDAFruitAndVegetables, USDAFruitAndVegetable.Symbols.Apples, Resolution.DAILY
        ).symbol
        self.strawberry_symbol = self.add_data(
            USDAFruitAndVegetables, USDAFruitAndVegetable.Symbols.Strawberries, Resolution.DAILY
        ).symbol
        self.broccoli_symbol = self.add_data(
            USDAFruitAndVegetables, USDAFruitAndVegetable.Symbols.Broccoli, Resolution.DAILY
        ).symbol

        # Track last data time for sparse data gap demonstration
        self.last_data_time = {}

        # Request historical data for warmup
        history = self.history(USDAFruitAndVegetables, self.apple_symbol, 30, Resolution.DAILY)
        self.debug(f"Received {len(history)} historical data points")

    def on_data(self, slice: Slice) -> None:
        """On data event handler"""
        # Demonstrate slice.contains_key for sparse data handling
        self._handle_sparse_data(slice, self.strawberry_symbol, "Strawberries")

        # Full data property access demonstration
        self._log_full_properties(slice, self.broccoli_symbol)

        # Fresh vs Frozen comparison
        self._compare_fresh_vs_frozen(slice, self.strawberry_symbol)

        #Filtering demonstration
        self._filter_by_form(slice, self.apple_symbol)

    def _handle_sparse_data(self, slice: Slice, symbol, name: str) -> None:
        """Demonstrate sparse data handling with time gap tracking."""
        if slice.contains_key(symbol):
            collection = slice[symbol]

            if symbol in self.last_data_time:
                days_since = (self.time - self.last_data_time[symbol]).days
                self.debug(f"{name}: Received data after {days_since} days gap")

            self.last_data_time[symbol] = self.time

            for data in collection.data:
                self.log(f"{self.time.year}: {name} ({data.form}) - ${data.price_per_cup_equivalent:.2f}/cup")

    def _log_full_properties(self, slice: Slice, symbol) -> None:
        """Demonstrate full data property access."""
        if slice.contains_key(symbol):
            collection = slice[symbol]

            for data in collection.data:
                # Access all available properties
                self.debug(f"--- {self.time.year} Broccoli ({data.form}) ---")
                if data.average_retail_price is not None:
                    self.debug(f"  Retail price: ${data.average_retail_price:.2f} {data.unit}")
                if data.preparation_yield_factor is not None:
                    self.debug(f"  Prep yield: {data.preparation_yield_factor:.1%} edible after prep")
                if data.cup_equivalent_size is not None:
                    self.debug(f"  Cup equivalent: {data.cup_equivalent_size} {data.cup_equivalent_unit}")
                self.debug(f"  Price per cup: ${data.value:.2f} (normalized)")

    def _compare_fresh_vs_frozen(self, slice: Slice, symbol) -> None:
        """Demonstrate Fresh vs Frozen price comparison."""
        if slice.contains_key(symbol):
            collection = slice[symbol]

            fresh_price = None
            frozen_price = None

            for data in collection.data:
                if data.form == "Fresh":
                    fresh_price = data.value
                elif data.form == "Frozen":
                    frozen_price = data.value

            if fresh_price and frozen_price:
                diff_pct = ((fresh_price - frozen_price) / frozen_price) * 100
                self.debug(f"{self.time.year}: Fresh ${fresh_price:.2f} vs Frozen ${frozen_price:.2f} ({diff_pct:+.1f}%)")

    def _filter_by_form(self, slice: Slice, symbol) -> None:
        """Demonstrate list comprehension filtering by form."""
        if slice.contains_key(symbol):
            collection = slice[symbol]

            #Get only Fresh forms
            fresh_data = [d for d in collection.data if d.form == "Fresh"]
            for data in fresh_data:
                self.debug(f"Fresh filter: {data.form} - ${data.price_per_cup_equivalent:.2f}/cup")

            #Get all Juice variants
            juice_data = [d for d in collection.data if "Juice" in d.form]
            if juice_data:
                self.debug(f"Found {len(juice_data)} juice variant(s)")
                for data in juice_data:
                    self.debug(f"Juice filter: {data.form} - ${data.price_per_cup_equivalent:.2f}/cup")

    def on_end_of_algorithm(self) -> None:
        """End of algorithm event"""
        self.debug(f"Algorithm completed. Final portfolio value: {self.portfolio.total_portfolio_value}")
