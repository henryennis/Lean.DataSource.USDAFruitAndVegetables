
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
 *
 */

using System.Diagnostics.CodeAnalysis;
using NodaTime;
using QuantConnect.Data;
using QuantConnect.Data.UniverseSelection;
using QuantConnect.Util;
using static QuantConnect.StringExtensions;

namespace QuantConnect.DataSource
{
    /// <summary>
    /// USDAFruitAndVegetables - Collection of USDA ERS retail price data for a product.
    /// Aggregates all product forms (Fresh, Canned, Frozen, etc.) under a single subscription.
    /// </summary>
    /// <remarks>
    /// This is the plural collection class extending <see cref="BaseDataCollection"/>.
    /// Individual data points are accessible via the <see cref="BaseDataCollection.Data"/> property
    /// as <see cref="USDAFruitAndVegetable"/> instances.
    ///
    /// Example usage:
    /// <code><![CDATA[
    /// AddData<USDAFruitAndVegetables>(USDAFruitAndVegetable.Symbols.Apples);
    ///
    /// // In OnData:
    /// foreach (var collection in slice.Get<USDAFruitAndVegetables>().Values)
    /// {
    ///     foreach (USDAFruitAndVegetable item in collection.Data)
    ///     {
    ///         Log($"{item.Form}: ${item.PricePerCupEquivalent}");
    ///     }
    /// }
    /// ]]></code>
    /// </remarks>
    public class USDAFruitAndVegetables : BaseDataCollection
    {
        private static readonly USDAFruitAndVegetable _factory = new();

        /// <summary>
        /// Return the URL source for the data
        /// </summary>
        /// <param name="config">Subscription data config</param>
        /// <param name="date">Date of the data</param>
        /// <param name="isLiveMode">Is this live mode</param>
        /// <returns>Subscription data source</returns>
        public override SubscriptionDataSource GetSource(SubscriptionDataConfig config, DateTime date, bool isLiveMode)
        {
            var productCode = config.Symbol.Value.ToLowerInvariant();
            var source = Path.Combine(
                Globals.DataFolder,
                "alternative",
                "usda",
                "fruitandvegetables",
                $"{productCode}.csv"
            );

            return new SubscriptionDataSource(source, SubscriptionTransportMedium.LocalFile, FileFormat.FoldingCollection);
        }

        /// <summary>
        /// Read and parse the data from a line, delegating to the factory instance.
        /// </summary>
        /// <param name="config">Subscription data config</param>
        /// <param name="line">Line of data</param>
        /// <param name="date">Date of the data</param>
        /// <param name="isLiveMode">Is this live mode</param>
        /// <returns>Parsed data object</returns>
        [return: MaybeNull]
        public override BaseData Reader(SubscriptionDataConfig config, string line, DateTime date, bool isLiveMode)
        {
            return _factory.Reader(config, line, date, isLiveMode);
        }

        /// <summary>
        /// Indicates whether this data requires mapping
        /// </summary>
        /// <returns>True if requires mapping, false otherwise</returns>
        public override bool RequiresMapping()
        {
            return _factory.RequiresMapping();
        }

        /// <summary>
        /// Indicates whether this is sparse data
        /// </summary>
        /// <returns>True if sparse, false otherwise</returns>
        public override bool IsSparseData()
        {
            return _factory.IsSparseData();
        }

        /// <summary>
        /// Gets the default resolution for this data
        /// </summary>
        /// <returns>The default resolution</returns>
        public override Resolution DefaultResolution()
        {
            return _factory.DefaultResolution();
        }

        /// <summary>
        /// Gets the supported resolutions for this data
        /// </summary>
        /// <returns>List of supported resolutions</returns>
        public override List<Resolution> SupportedResolutions()
        {
            return _factory.SupportedResolutions();
        }

        /// <summary>
        /// Gets the data timezone for this data
        /// </summary>
        /// <returns>The timezone</returns>
        public override DateTimeZone DataTimeZone()
        {
            return _factory.DataTimeZone();
        }

        /// <summary>
        /// Creates a deep clone of this collection, including all data points.
        /// </summary>
        /// <returns>A clone of this collection with cloned data points</returns>
        public override BaseData Clone()
        {
            return new USDAFruitAndVegetables()
            {
                Symbol = Symbol,
                Time = Time,
                EndTime = EndTime,
                Data = Data?.ToList(point => point.Clone()),
            };
        }

        /// <summary>
        /// Returns a string representation of this collection
        /// </summary>
        /// <returns>String representation</returns>
        public override string ToString()
        {
            if (Data == null || Data.Count == 0)
            {
                return Invariant($"{Symbol} - Empty collection");
            }
            return Invariant($"{Symbol} - [{string.Join(", ", Data.Select(d => d.ToString()))}]");
        }
    }
}
