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
using System.Runtime.Serialization;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using NodaTime;
using QuantConnect.Data;
using static QuantConnect.StringExtensions;

namespace QuantConnect.DataSource
{
    /// <summary>
    /// USDAFruitAndVegetables - USDA ERS retail price estimates for 150+ commonly consumed fresh and
    /// processed fruits and vegetables. Includes price per pound/pint and normalized price per edible
    /// cup equivalent.
    /// </summary>
    /// <remarks>
    /// Data source ID: 0
    /// </remarks>
    /// <remarks>
    /// CSV Format (7 columns):
    /// yyyyMMdd,AverageRetailPrice,Unit,PreparationYieldFactor,CupEquivalentSize,CupEquivalentUnit,PricePerCupEquivalent
    ///
    /// Example:
    /// 20130101,1.5675153914496354,per_pound,0.9,0.24250848840336534,pounds,0.42237309792162286
    /// </remarks>
    public partial class USDAFruitAndVegetables : BaseData
    {
        /// <summary>
        /// Data source ID for USDA FruitAndVegetables
        /// </summary>
        public const int DataSourceId = 0;

        /// <summary>
        /// CSV schema definition for USDA Fruit and Vegetables data.
        /// </summary>

        internal static class CsvSchema
        {
            /// <summary>Total number of columns in CSV format.</summary>
            public const int ColumnCount = 7;

            /// <summary>Column 0: Date in yyyyMMdd format.</summary>
            public const int DateIndex = 0;
            /// <summary>Column 1: Average retail price per unit (pound or pint) (nullable)</summary>
            public const int AverageRetailPriceIndex = 1;
            /// <summary>Column 2: Unit of measure - PerPound (solids) or PerPint (juice) (nullable)</summary>
            public const int UnitIndex = 2;
            /// <summary>Column 3: Fraction of product that is edible after preparation (0.0-1.0) (nullable)</summary>
            public const int PreparationYieldFactorIndex = 3;
            /// <summary>Column 4: Size of one edible cup equivalent (nullable)</summary>
            public const int CupEquivalentSizeIndex = 4;
            /// <summary>Column 5: Unit of measure for CupEquivalentSize (pounds, pints, fluid_ounces) (nullable)</summary>
            public const int CupEquivalentUnitIndex = 5;
            /// <summary>Column 6: Normalized price per edible cup equivalent (comparable across forms) (nullable)</summary>
            public const int PricePerCupEquivalentIndex = 6;

            public const string PriceUnitPerPound = "per_pound";
            public const string PriceUnitPerPint = "per_pint";

            public const string CupEquivalentUnitPounds = "pounds";
            public const string CupEquivalentUnitPints = "pints";
            public const string CupEquivalentUnitFluidOunces = "fluid_ounces";
        }

        /// <summary>
        /// Average retail price per unit (pound or pint).
        /// </summary>
        public decimal? AverageRetailPrice { get; set; }

        /// <summary>
        /// Unit of measure - PerPound (solids) or PerPint (juice).
        /// </summary>
        public PriceUnit? Unit { get; set; }

        /// <summary>
        /// Fraction of product that is edible after preparation (0.0-1.0).
        /// </summary>
        public decimal? PreparationYieldFactor { get; set; }

        /// <summary>
        /// Size of one edible cup equivalent.
        /// </summary>
        public decimal? CupEquivalentSize { get; set; }

        /// <summary>
        /// Unit of measure for <see cref="CupEquivalentSize"/>.
        /// </summary>
        public CupEquivalentUnit? CupEquivalentUnit { get; set; }

        /// <summary>
        /// Normalized price per edible cup equivalent (comparable across forms).
        /// </summary>
        public decimal? PricePerCupEquivalent { get; set; }

        /// <summary>
        /// Returns the primary value (PricePerCupEquivalent), or 0 if null.
        /// </summary>
        /// <remarks>
        /// PricePerCupEquivalent is chosen over AverageRetailPrice because it enables
        /// meaningful cross-form comparisons. A pound of fresh apples and a pint of
        /// apple juice have different retail prices, but their price-per-cup-equivalent
        /// normalizes for edible portion size, making cost comparisons valid across
        /// fresh, canned, dried, and juice forms of the same product.
        ///
        /// Returns 0 when PricePerCupEquivalent is null to ensure chart data
        /// always has a numeric value. Users should check HasValue on the underlying
        /// property when null-awareness is needed.
        /// </remarks>
        public override decimal Value => PricePerCupEquivalent ?? 0m;

        /// <summary>
        /// Return the URL source for the data
        /// </summary>
        /// <param name="config">Subscription data config</param>
        /// <param name="date">Date of the data</param>
        /// <param name="isLiveMode">Is this live mode</param>
        /// <returns>Subscription data source</returns>
        public override SubscriptionDataSource GetSource(SubscriptionDataConfig config, DateTime date, bool isLiveMode)
        {

            var seriesCode = config.Symbol.Value.ToLowerInvariant();
            var source = Path.Combine(
                Globals.DataFolder,
                "alternative",
                "usda",
                "fruitandvegetables",
                $"{seriesCode}.csv"
            );

            return new SubscriptionDataSource(source, SubscriptionTransportMedium.LocalFile, FileFormat.Csv);

        }


        /// <summary>
        /// Read and parse the data from a line
        /// </summary>
        /// <param name="config">Subscription data config</param>
        /// <param name="line">Line of data</param>
        /// <param name="date">Date of the data</param>
        /// <param name="isLiveMode">Is this live mode</param>
        /// <returns>Parsed data object</returns>
        /// <seealso cref="CsvSchema"/>
        [return: MaybeNull]
        public override BaseData Reader(SubscriptionDataConfig config, string line, DateTime date, bool isLiveMode)
        {
            if (string.IsNullOrWhiteSpace(line))
            {
                throw new FormatException("Encountered empty line");
            }

            var columns = line.Split(',');

            if (columns.Length != CsvSchema.ColumnCount)
            {
                throw new FormatException(
                    $"Invalid CSV format, expected {CsvSchema.ColumnCount} columns but got {columns.Length}"
                );
            }
            var dateText = columns[CsvSchema.DateIndex].Trim();
            if (dateText.Length != 8 || !long.TryParse(dateText, out _))
            {
                throw new FormatException(
                    "Invalid CSV format, expected yyyyMMdd date in first column"
                );
            }

            var time = Parse.DateTimeExact(dateText, "yyyyMMdd");

            // Parse nullable decimal fields
            var averageRetailPrice = TryParseNullableDecimal(columns[CsvSchema.AverageRetailPriceIndex]);
            var preparationYieldFactor = TryParseNullableDecimal(columns[CsvSchema.PreparationYieldFactorIndex]);
            var cupEquivalentSize = TryParseNullableDecimal(columns[CsvSchema.CupEquivalentSizeIndex]);
            var pricePerCupEquivalent = TryParseNullableDecimal(columns[CsvSchema.PricePerCupEquivalentIndex]);

            // Parse nullable enum fields (empty string → null)
            var unit = TryParsePriceUnit(columns[CsvSchema.UnitIndex]);
            var cupEquivalentUnit = TryParseCupEquivalentUnit(columns[CsvSchema.CupEquivalentUnitIndex]);

            return new USDAFruitAndVegetables()
            {
                Symbol = config.Symbol,
                Time = time,
                AverageRetailPrice = averageRetailPrice,
                Unit = unit,
                PreparationYieldFactor = preparationYieldFactor,
                CupEquivalentSize = cupEquivalentSize,
                CupEquivalentUnit = cupEquivalentUnit,
                PricePerCupEquivalent = pricePerCupEquivalent,
            };
        }

        /// <summary>
        /// Indicates whether this data requires mapping
        /// </summary>
        /// <returns>True if requires mapping, false otherwise</returns>
        public override bool RequiresMapping()
        {
            return false;
        }

        /// <summary>
        /// Indicates whether this is sparse data
        /// </summary>
        /// <returns>True if sparse, false otherwise</returns>
        public override bool IsSparseData()
        {
            return true;
        }

        /// <summary>
        /// Gets the default resolution for this data
        /// </summary>
        /// <returns>The default resolution</returns>
        public override Resolution DefaultResolution()
        {
            return Resolution.Daily;
        }

        /// <summary>
        /// Gets the supported resolutions for this data
        /// </summary>
        /// <returns>List of supported resolutions</returns>
        public override List<Resolution> SupportedResolutions()
        {
            return DailyResolution;
        }

        /// <summary>
        /// Gets the data timezone for this data
        /// </summary>
        /// <returns>The timezone</returns>
        public override DateTimeZone DataTimeZone()
        {
            return TimeZones.Utc;
        }

        /// <summary>
        /// Creates a clone of this instance
        /// </summary>
        /// <returns>A clone of this instance</returns>
        public override BaseData Clone()
        {
            return new USDAFruitAndVegetables()
            {
                Symbol = Symbol,
                Time = Time,
                AverageRetailPrice = AverageRetailPrice,
                Unit = Unit,
                PreparationYieldFactor = PreparationYieldFactor,
                CupEquivalentSize = CupEquivalentSize,
                CupEquivalentUnit = CupEquivalentUnit,
                PricePerCupEquivalent = PricePerCupEquivalent,
            };
        }

        /// <summary>
        /// Returns a string representation of this data
        /// </summary>
        /// <returns>String representation</returns>
        public override string ToString()
        {

            return Invariant($"{Symbol} - AverageRetailPrice: {AverageRetailPrice}, Unit: {Unit}, PreparationYieldFactor: {PreparationYieldFactor}, CupEquivalentSize: {CupEquivalentSize}, CupEquivalentUnit: {CupEquivalentUnit}, PricePerCupEquivalent: {PricePerCupEquivalent}");

        }

        /// <summary>
        /// Parse a nullable decimal from CSV. Empty string returns null.
        /// </summary>
        private static decimal? TryParseNullableDecimal(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return null;
            }

            if (!decimal.TryParse(value.Trim(), out var result))
            {
                throw new FormatException($"Invalid decimal value '{value}'");
            }
            return result;
        }

        /// <summary>
        /// Parse price unit. Returns null if empty string.
        /// </summary>
        private static PriceUnit? TryParsePriceUnit(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return null;
            }

            var normalized = value.Trim().ToLowerInvariant();
            return normalized switch
            {
                CsvSchema.PriceUnitPerPound => PriceUnit.PerPound,
                CsvSchema.PriceUnitPerPint => PriceUnit.PerPint,
                _ => null
            };
        }

        /// <summary>
        /// Parse cup equivalent unit. Returns null if empty string.
        /// </summary>
        private static CupEquivalentUnit? TryParseCupEquivalentUnit(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return null;
            }

            var normalized = value.Trim().ToLowerInvariant();
            return normalized switch
            {
                CsvSchema.CupEquivalentUnitPounds => DataSource.CupEquivalentUnit.Pounds,
                CsvSchema.CupEquivalentUnitPints => DataSource.CupEquivalentUnit.Pints,
                CsvSchema.CupEquivalentUnitFluidOunces => DataSource.CupEquivalentUnit.FluidOunces,
                _ => null
            };
        }
    }

    /// <summary>
    /// Unit of measure for retail price.
    /// </summary>
    [JsonConverter(typeof(StringEnumConverter))]
    public enum PriceUnit
    {
        /// <summary>Price per pound (used for solid products).</summary>
        [EnumMember(Value = "per_pound")]
        PerPound,

        /// <summary>Price per pint (used for juice products).</summary>
        [EnumMember(Value = "per_pint")]
        PerPint,
    }

    /// <summary>
    /// Unit of measure for cup equivalent size.
    /// </summary>
    [JsonConverter(typeof(StringEnumConverter))]
    public enum CupEquivalentUnit
    {
        /// <summary>Cup equivalent measured in pounds.</summary>
        [EnumMember(Value = "pounds")]
        Pounds,

        /// <summary>Cup equivalent measured in pints.</summary>
        [EnumMember(Value = "pints")]
        Pints,

        /// <summary>Cup equivalent measured in fluid ounces.</summary>
        [EnumMember(Value = "fluid_ounces")]
        FluidOunces,
    }
}
