/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using NodaTime;
using NUnit.Framework;
using QuantConnect.Data;

namespace QuantConnect.DataSource.Tests
{
    [TestFixture]
    public class USDAFruitAndVegetableTests
    {
        private USDAFruitAndVegetable _instance = null!;
        private SubscriptionDataConfig _config = null!;

        [SetUp]
        public void SetUp()
        {
            _instance = new USDAFruitAndVegetable();
            _config = new SubscriptionDataConfig(
                typeof(USDAFruitAndVegetables),
                Symbol.Create(USDAFruitAndVegetable.Symbols.Apples, SecurityType.Base, Market.USA),
                Resolution.Daily,
                DateTimeZoneProviders.Tzdb["UTC"],
                DateTimeZoneProviders.Tzdb["UTC"],
                false,
                false,
                false
            );
        }

        // === Happy Path ===

        [Test]
        public void ReaderParsesSampleOutputLine()
        {
            var line = "20130101,Fresh,1.5675153914496354,per_pound,0.9,0.24250848840336534,pounds,0.42237309792162286";
            var date = new DateTime(2013, 1, 1);

            var result = _instance.Reader(_config, line, date, false) as USDAFruitAndVegetable;

            Assert.That(result, Is.Not.Null);
            var data = result!;
            Assert.That(data.Symbol, Is.EqualTo(_config.Symbol));
            Assert.That(data.Time, Is.EqualTo(date));
            Assert.That(data.EndTime, Is.EqualTo(date), "EndTime equals Time (BaseData default)");
            Assert.That(data.Form, Is.EqualTo("Fresh"));
            Assert.That(data.AverageRetailPrice, Is.EqualTo(1.5675153914496354m));
            Assert.That(data.Unit, Is.EqualTo(PriceUnit.PerPound));
            Assert.That(data.PreparationYieldFactor, Is.EqualTo(0.9m));
            Assert.That(data.CupEquivalentSize, Is.EqualTo(0.24250848840336534m));
            Assert.That(data.CupEquivalentUnit, Is.EqualTo(CupEquivalentUnit.Pounds));
            Assert.That(data.PricePerCupEquivalent, Is.EqualTo(0.42237309792162286m));
            Assert.That(data.Value, Is.EqualTo(0.42237309792162286m));
        }

        [Test]
        public void ReaderParsesFormWithSemicolon()
        {
            // DataProcessing sanitizes commas to semicolons (e.g., "Juice, Ready to drink" -> "Juice; Ready to drink")
            // This ensures CSV parsing with simple split(',') works correctly
            var line = "20200101,Juice; Ready to drink,2.50,per_pint,1.0,0.5,pints,2.50";
            var date = new DateTime(2020, 1, 1);

            var result = _instance.Reader(_config, line, date, false) as USDAFruitAndVegetable;

            Assert.That(result, Is.Not.Null);
            Assert.That(result!.Form, Is.EqualTo("Juice; Ready to drink"));
        }

        // === Error Handling (Fail-Fast) ===

        [TestCase(" ", "Empty line", TestName = "ReaderThrowsOnEmptyLine")]
        [TestCase("20200101,Fresh,1.23,per_pound", "Invalid column count", TestName = "ReaderThrowsOnInvalidColumnCount")]
        [TestCase("2020-01-01,Fresh,1.23,per_pound,0.9,0.25,pounds,0.50", "Invalid date format", TestName = "ReaderThrowsOnInvalidDateFormat")]
        [TestCase("20200132,Fresh,1.23,per_pound,0.9,0.25,pounds,0.50", "Invalid day in date", TestName = "ReaderThrowsOnInvalidDayInDate")]
        [TestCase("20200101,Fresh,abc,per_pound,0.9,0.25,pounds,0.50", "Non-numeric price", TestName = "ReaderThrowsOnNonNumericPrice")]
        [TestCase("20200101,Fresh,1.23,per_pound,invalid,0.25,pounds,0.50", "Non-numeric yield factor", TestName = "ReaderThrowsOnNonNumericYieldFactor")]
        public void ReaderThrowsOnInvalidInput(string line, string errorType)
        {
            var date = new DateTime(2020, 1, 1);
            Assert.Throws<FormatException>(() => _instance.Reader(_config, line, date, false),
                $"Expected FormatException for: {errorType}");
        }

        // === Enum Parsing ===

        [TestCase("per_pound", "PerPound", TestName = "ReaderParsesPriceUnit_PerPound")]
        [TestCase("per_pint", "PerPint", TestName = "ReaderParsesPriceUnit_PerPint")]
        public void ReaderParsesPriceUnitValues(string unitValue, string expectedEnumName)
        {
            var line = $"20200101,Fresh,1.23,{unitValue},0.9,0.25,pounds,0.50";
            var date = new DateTime(2020, 1, 1);

            var result = _instance.Reader(_config, line, date, false) as USDAFruitAndVegetable;

            Assert.That(result, Is.Not.Null);
            Assert.That(result!.Unit, Is.EqualTo(Enum.Parse<PriceUnit>(expectedEnumName)));
        }

        [TestCase("pounds", "Pounds", TestName = "ReaderParsesCupEquivalentUnit_Pounds")]
        [TestCase("pints", "Pints", TestName = "ReaderParsesCupEquivalentUnit_Pints")]
        [TestCase("fluid_ounces", "FluidOunces", TestName = "ReaderParsesCupEquivalentUnit_FluidOunces")]
        public void ReaderParsesCupEquivalentUnitValues(string unitValue, string expectedEnumName)
        {
            var line = $"20200101,Fresh,1.23,per_pound,0.9,0.25,{unitValue},0.50";
            var date = new DateTime(2020, 1, 1);

            var result = _instance.Reader(_config, line, date, false) as USDAFruitAndVegetable;

            Assert.That(result, Is.Not.Null);
            Assert.That(result!.CupEquivalentUnit, Is.EqualTo(Enum.Parse<CupEquivalentUnit>(expectedEnumName)));
        }

        [TestCase("unknown_unit", null, "pounds", "Pounds", TestName = "ReaderReturnsNullForUnknownPriceUnit")]
        [TestCase("per_pound", "PerPound", "unknown_unit", null, TestName = "ReaderReturnsNullForUnknownCupEquivalentUnit")]
        public void ReaderReturnsNullForUnknownEnumValues(string priceUnit, string? expectedPriceUnitName, string cupUnit, string? expectedCupUnitName)
        {
            var line = $"20200101,Fresh,1.23,{priceUnit},0.9,0.25,{cupUnit},0.50";
            var date = new DateTime(2020, 1, 1);

            var result = _instance.Reader(_config, line, date, false) as USDAFruitAndVegetable;

            Assert.That(result, Is.Not.Null);
            var expectedPriceUnit = expectedPriceUnitName == null ? null : (PriceUnit?)Enum.Parse<PriceUnit>(expectedPriceUnitName);
            var expectedCupUnit = expectedCupUnitName == null ? null : (CupEquivalentUnit?)Enum.Parse<CupEquivalentUnit>(expectedCupUnitName);
            Assert.That(result!.Unit, Is.EqualTo(expectedPriceUnit), "Price unit should match expected");
            Assert.That(result.CupEquivalentUnit, Is.EqualTo(expectedCupUnit), "Cup unit should match expected");
            Assert.That(result.AverageRetailPrice, Is.EqualTo(1.23m), "Other fields still parsed");
        }

        // === Nullable Field Parsing ===

        // Records with valid PricePerCupEquivalent (primary value) should be parsed
        [TestCase("20200101,Fresh,1.23,,0.9,0.25,pounds,0.50", 1.23, null, 0.9, 0.25, "Pounds", 0.50, TestName = "ReaderAcceptsNullUnitWhenPricePresent")]
        [TestCase("20200101,Fresh,1.23,per_pound,0.9,0.25,,0.50", 1.23, "PerPound", 0.9, 0.25, null, 0.50, TestName = "ReaderAcceptsNullCupUnitWhenSizePresent")]
        [TestCase("20200101,Fresh,,per_pound,0.9,,pounds,0.50", null, "PerPound", 0.9, null, "Pounds", 0.50, TestName = "ReaderAcceptsUnitWithoutValue")]
        public void ReaderParsesNullableFieldCombinations(
            string line,
            double? expectedPrice,
            string? expectedUnit,
            double? expectedYield,
            double? expectedCupSize,
            string? expectedCupUnit,
            double? expectedPricePerCup)
        {
            var date = new DateTime(2020, 1, 1);

            var result = _instance.Reader(_config, line, date, false) as USDAFruitAndVegetable;

            Assert.That(result, Is.Not.Null);
            var data = result!;
            Assert.That(data.Form, Is.EqualTo("Fresh"));
            Assert.That(data.AverageRetailPrice, Is.EqualTo(expectedPrice == null ? null : (decimal?)expectedPrice));
            Assert.That(data.Unit, Is.EqualTo(expectedUnit == null ? null : Enum.Parse<PriceUnit>(expectedUnit)));
            Assert.That(data.PreparationYieldFactor, Is.EqualTo(expectedYield == null ? null : (decimal?)expectedYield));
            Assert.That(data.CupEquivalentSize, Is.EqualTo(expectedCupSize == null ? null : (decimal?)expectedCupSize));
            Assert.That(data.CupEquivalentUnit, Is.EqualTo(expectedCupUnit == null ? null : Enum.Parse<CupEquivalentUnit>(expectedCupUnit)));
            Assert.That(data.PricePerCupEquivalent, Is.EqualTo(expectedPricePerCup == null ? null : (decimal?)expectedPricePerCup));
        }

        [Test]
        public void ReaderParsesNullPricePerCupEquivalent()
        {
            // Per ADR-0007: nullable fields are allowed, Reader returns object with null value
            var line = "20200101,Fresh,1.23,per_pound,0.9,0.25,pounds,";
            var date = new DateTime(2020, 1, 1);

            var result = _instance.Reader(_config, line, date, false) as USDAFruitAndVegetable;

            Assert.That(result, Is.Not.Null);
            Assert.That(result!.PricePerCupEquivalent, Is.Null, "PricePerCupEquivalent should be null for empty field");
            Assert.That(result.Value, Is.EqualTo(0m), "Value falls back to 0 when PricePerCupEquivalent is null");
            Assert.That(result.AverageRetailPrice, Is.EqualTo(1.23m), "Other fields still parsed");
        }

        // === Metadata Contract ===

        [TestCase("SupportedResolutions")]
        [TestCase("DataTimeZone")]
        [TestCase("RequiresMapping")]
        [TestCase("IsSparseData")]
        [TestCase("DefaultResolution")]
        public void MetadataContractReturnsExpectedValues(string property)
        {
            switch (property)
            {
                case "SupportedResolutions":
                    Assert.That(_instance.SupportedResolutions(), Is.EquivalentTo(new[] { Resolution.Daily }));
                    break;
                case "DataTimeZone":
                    Assert.That(_instance.DataTimeZone(), Is.EqualTo(TimeZones.Utc));
                    break;
                case "RequiresMapping":
                    Assert.That(_instance.RequiresMapping(), Is.False);
                    break;
                case "IsSparseData":
                    Assert.That(_instance.IsSparseData(), Is.True);
                    break;
                case "DefaultResolution":
                    Assert.That(_instance.DefaultResolution(), Is.EqualTo(Resolution.Daily));
                    break;
            }
        }

        // === Clone ===

        [TestCase(false, TestName = "ClonePreservesAllFieldValues")]
        [TestCase(true, TestName = "ClonePreservesNullValues")]
        public void ClonePreservesFieldValues(bool useNullValues)
        {
            _instance.Time = new DateTime(2020, 1, 1);
            _instance.EndTime = _instance.Time.AddDays(1);
            _instance.Form = "Fresh";

            if (useNullValues)
            {
                _instance.AverageRetailPrice = null;
                _instance.Unit = null;
                _instance.PreparationYieldFactor = 0.9m;
                _instance.CupEquivalentSize = null;
                _instance.CupEquivalentUnit = null;
                _instance.PricePerCupEquivalent = null;
            }
            else
            {
                _instance.AverageRetailPrice = 1.23m;
                _instance.Unit = PriceUnit.PerPound;
                _instance.PreparationYieldFactor = 0.9m;
                _instance.CupEquivalentSize = 0.25m;
                _instance.CupEquivalentUnit = CupEquivalentUnit.Pounds;
                _instance.PricePerCupEquivalent = 0.50m;
            }

            var clone = (USDAFruitAndVegetable)_instance.Clone();

            Assert.That(clone, Is.Not.Null);
            Assert.That(clone, Is.Not.SameAs(_instance));
            Assert.That(clone.Time, Is.EqualTo(_instance.Time));
            Assert.That(clone.EndTime, Is.EqualTo(_instance.EndTime));
            Assert.That(clone.Form, Is.EqualTo(_instance.Form));
            Assert.That(clone.AverageRetailPrice, Is.EqualTo(_instance.AverageRetailPrice));
            Assert.That(clone.Unit, Is.EqualTo(_instance.Unit));
            Assert.That(clone.PreparationYieldFactor, Is.EqualTo(_instance.PreparationYieldFactor));
            Assert.That(clone.CupEquivalentSize, Is.EqualTo(_instance.CupEquivalentSize));
            Assert.That(clone.CupEquivalentUnit, Is.EqualTo(_instance.CupEquivalentUnit));
            Assert.That(clone.PricePerCupEquivalent, Is.EqualTo(_instance.PricePerCupEquivalent));
        }
    }
}
