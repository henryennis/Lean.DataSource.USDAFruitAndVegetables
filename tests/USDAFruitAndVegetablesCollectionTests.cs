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
    public class USDAFruitAndVegetablesCollectionTests
    {
        private USDAFruitAndVegetables _collection = null!;
        private USDAFruitAndVegetable _factory = null!;
        private SubscriptionDataConfig _config = null!;

        [SetUp]
        public void SetUp()
        {
            _collection = new USDAFruitAndVegetables();
            _factory = new USDAFruitAndVegetable();
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

        // === GetSource Contract ===

        [Test]
        public void GetSourceReturnsProductLevelPath()
        {
            var source = _collection.GetSource(_config, new DateTime(2020, 1, 1), false);

            Assert.That(
                source.Source,
                Does.EndWith(Path.Combine("alternative", "usda", "fruitandvegetables", "apples.csv"))
            );
            Assert.That(source.TransportMedium, Is.EqualTo(SubscriptionTransportMedium.LocalFile));
            Assert.That(source.Format, Is.EqualTo(FileFormat.FoldingCollection));
        }

        // === Reader Delegation ===

        [Test]
        public void ReaderDelegatesToFactory()
        {
            var line = "20200101,Fresh,1.23,per_pound,0.9,0.25,pounds,0.50";
            var date = new DateTime(2020, 1, 1);

            var result = _collection.Reader(_config, line, date, false) as USDAFruitAndVegetable;

            Assert.That(result, Is.Not.Null);
            Assert.That(result!.Form, Is.EqualTo("Fresh"));
            Assert.That(result.AverageRetailPrice, Is.EqualTo(1.23m));
        }

        // === Metadata Delegation ===

        [Test]
        public void CollectionDelegatesAllMetadataToFactory()
        {
            // Collection should delegate all metadata methods to factory
            // Verify by comparing results (same implementation)
            Assert.That(_collection.RequiresMapping(), Is.EqualTo(_factory.RequiresMapping()));
            Assert.That(_collection.IsSparseData(), Is.EqualTo(_factory.IsSparseData()));
            Assert.That(_collection.DefaultResolution(), Is.EqualTo(_factory.DefaultResolution()));
            Assert.That(_collection.SupportedResolutions(), Is.EquivalentTo(_factory.SupportedResolutions()));
            Assert.That(_collection.DataTimeZone(), Is.EqualTo(_factory.DataTimeZone()));
        }

        // === Clone ===

        [Test]
        public void CloneDeepCopiesDataCollection()
        {
            _collection.Symbol = _config.Symbol;
            _collection.Time = new DateTime(2020, 1, 1);
            _collection.EndTime = new DateTime(2020, 1, 1);

            var item1 = new USDAFruitAndVegetable
            {
                Symbol = _config.Symbol,
                Time = new DateTime(2020, 1, 1),
                Form = "Fresh",
                AverageRetailPrice = 1.23m,
                PricePerCupEquivalent = 0.50m,
            };
            var item2 = new USDAFruitAndVegetable
            {
                Symbol = _config.Symbol,
                Time = new DateTime(2020, 1, 1),
                Form = "Applesauce",
                AverageRetailPrice = 2.34m,
                PricePerCupEquivalent = 0.75m,
            };
            _collection.Data = new List<BaseData> { item1, item2 };

            var clone = _collection.Clone() as USDAFruitAndVegetables;

            Assert.That(clone, Is.Not.Null);
            Assert.That(clone, Is.Not.SameAs(_collection));
            Assert.That(clone!.Data, Is.Not.Null);
            Assert.That(clone.Data!.Count, Is.EqualTo(2));
            Assert.That(clone.Data, Is.Not.SameAs(_collection.Data));

            // Verify deep copy - items should be different instances
            var clonedItem1 = clone.Data[0] as USDAFruitAndVegetable;
            var clonedItem2 = clone.Data[1] as USDAFruitAndVegetable;
            Assert.That(clonedItem1, Is.Not.SameAs(item1));
            Assert.That(clonedItem2, Is.Not.SameAs(item2));
            Assert.That(clonedItem1!.Form, Is.EqualTo("Fresh"));
            Assert.That(clonedItem2!.Form, Is.EqualTo("Applesauce"));
        }

        [TestCase(false, TestName = "CloneHandlesEmptyDataCollection")]
        [TestCase(true, TestName = "CloneHandlesNullDataCollection")]
        public void CloneHandlesEdgeCaseData(bool useNullData)
        {
            _collection.Symbol = _config.Symbol;
            _collection.Time = new DateTime(2020, 1, 1);
            _collection.Data = useNullData ? null : new List<BaseData>();

            var clone = _collection.Clone() as USDAFruitAndVegetables;

            Assert.That(clone, Is.Not.Null);
            if (useNullData)
            {
                Assert.That(clone!.Data, Is.Null);
            }
            else
            {
                Assert.That(clone!.Data, Is.Not.Null);
                Assert.That(clone.Data!.Count, Is.EqualTo(0));
            }
        }

        // === ToString ===

        [TestCase(null, TestName = "ToStringReturnsEmptyForNullData")]
        [TestCase(false, TestName = "ToStringReturnsEmptyForEmptyData")]
        [TestCase(true, TestName = "ToStringIncludesAllDataItems")]
        public void ToStringHandlesAllStates(bool? hasData)
        {
            _collection.Symbol = _config.Symbol;

            if (hasData == null)
            {
                _collection.Data = null;
            }
            else if (hasData == false)
            {
                _collection.Data = new List<BaseData>();
            }
            else
            {
                var item1 = new USDAFruitAndVegetable { Form = "Fresh", PricePerCupEquivalent = 0.50m };
                var item2 = new USDAFruitAndVegetable { Form = "Applesauce", PricePerCupEquivalent = 0.75m };
                _collection.Data = new List<BaseData> { item1, item2 };
            }

            var result = _collection.ToString();

            if (hasData == true)
            {
                Assert.That(result, Does.Contain("Fresh"));
                Assert.That(result, Does.Contain("Applesauce"));
            }
            else
            {
                Assert.That(result, Does.Contain("Empty collection"));
            }
        }

        // === Collection Semantics ===

        [Test]
        public void MultipleFormsCanBeParsedIntoCollection()
        {
            var lines = new[]
            {
                "20200101,Fresh,1.23,per_pound,0.9,0.25,pounds,0.50",
                "20200101,Applesauce,2.34,per_pound,1.0,0.30,pounds,0.75",
                "20200101,Juice Frozen,3.45,per_pint,1.0,0.50,pints,1.00",
            };
            var date = new DateTime(2020, 1, 1);

            var parsedItems = new List<USDAFruitAndVegetable>();
            foreach (var line in lines)
            {
                var result = _collection.Reader(_config, line, date, false) as USDAFruitAndVegetable;
                Assert.That(result, Is.Not.Null);
                parsedItems.Add(result!);
            }

            Assert.That(parsedItems.Count, Is.EqualTo(3));
            Assert.That(parsedItems[0].Form, Is.EqualTo("Fresh"));
            Assert.That(parsedItems[1].Form, Is.EqualTo("Applesauce"));
            Assert.That(parsedItems[2].Form, Is.EqualTo("Juice Frozen"));
        }
    }
}
