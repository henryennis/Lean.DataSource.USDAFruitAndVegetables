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

using System.Reflection;
using System.Text.RegularExpressions;
using NodaTime;
using NUnit.Framework;
using QuantConnect.Data;
using QuantConnect.Util;

namespace QuantConnect.DataSource.Tests
{
    /// <summary>
    /// Integration tests that validate the Reader against actual processed CSV files.
    /// These tests use Globals.DataFolder (configured via tests/config.json) to locate sample data.
    /// </summary>
    /// <remarks>
    /// Prerequisites:
    /// - Run DataProcessing to populate output/ directory with CSV files
    /// - tests/config.json must define "data-folder": "../output/"
    /// - TestSetup.cs must call Globals.Reset() after Config.Reset()
    ///
    /// If no sample data exists, tests are skipped with Assert.Ignore().
    /// </remarks>
    [TestFixture]
    public class USDAFruitAndVegetablesIntegrationTests
    {
        private string _dataPath = null!;
        private USDAFruitAndVegetable _instance = null!;

        [SetUp]
        public void SetUp()
        {
            _instance = new USDAFruitAndVegetable();
            _dataPath = Path.Combine(Globals.DataFolder, "alternative", "usda", "fruitandvegetables");
        }

        private void SkipIfNoData()
        {
            if (!Directory.Exists(_dataPath) || !Directory.EnumerateFiles(_dataPath, "*.csv").Any())
            {
                Assert.Ignore($"No sample data found in {_dataPath}. Run 'make process' to generate test data.");
            }
        }

        private static SubscriptionDataConfig CreateConfig(string productCode)
        {
            return new SubscriptionDataConfig(
                typeof(USDAFruitAndVegetables),
                Symbol.Create(productCode, SecurityType.Base, Market.USA),
                Resolution.Daily,
                DateTimeZoneProviders.Tzdb["UTC"],
                DateTimeZoneProviders.Tzdb["UTC"],
                false,
                false,
                false
            );
        }

        // === Sample File Parsing Tests ===

        [Test]
        public void ParsesApplesFileWithoutExceptions()
        {
            SkipIfNoData();

            var filePath = Path.Combine(_dataPath, "apples.csv");
            if (!File.Exists(filePath))
            {
                Assert.Ignore("apples.csv not found in sample data");
            }

            var config = CreateConfig("apples");
            var lines = File.ReadAllLines(filePath);
            var parsedCount = 0;

            foreach (var line in lines)
            {
                var result = _instance.Reader(config, line, DateTime.UtcNow, false) as USDAFruitAndVegetable;
                Assert.That(result, Is.Not.Null, $"Failed to parse line: {line}");
                Assert.That(result!.Symbol, Is.EqualTo(config.Symbol));
                Assert.That(result.Time, Is.Not.EqualTo(default(DateTime)));
                Assert.That(result.Form, Is.Not.Null.And.Not.Empty);
                parsedCount++;
            }

            Assert.That(parsedCount, Is.GreaterThan(0), "No rows parsed from apples.csv");
            TestContext.WriteLine($"Successfully parsed {parsedCount} rows from apples.csv");
        }

        [Test]
        public void ParsesAtLeastThreeProductFilesSuccessfully()
        {
            SkipIfNoData();

            var csvFiles = Directory.GetFiles(_dataPath, "*.csv").Take(5).ToList();
            if (csvFiles.Count < 3)
            {
                Assert.Ignore($"Need at least 3 CSV files for this test, found {csvFiles.Count}");
            }

            var totalParsed = 0;
            var filesParsed = 0;

            foreach (var filePath in csvFiles.Take(3))
            {
                var productCode = Path.GetFileNameWithoutExtension(filePath);
                var config = CreateConfig(productCode);
                var lines = File.ReadAllLines(filePath);

                foreach (var line in lines)
                {
                    var result = _instance.Reader(config, line, DateTime.UtcNow, false) as USDAFruitAndVegetable;
                    Assert.That(result, Is.Not.Null, $"Failed to parse line in {productCode}.csv: {line}");
                    Assert.That(result!.Symbol, Is.Not.Null);
                    Assert.That(result.Time, Is.Not.EqualTo(default(DateTime)));
                    Assert.That(result.Form, Is.Not.Null.And.Not.Empty);
                    totalParsed++;
                }
                filesParsed++;
            }

            Assert.That(filesParsed, Is.EqualTo(3));
            Assert.That(totalParsed, Is.GreaterThan(0));
            TestContext.WriteLine($"Successfully parsed {totalParsed} total rows from {filesParsed} product files");
        }

        // === Form Consistency Validation ===

        [Test]
        public void DetectsFormNameVariationsAcrossYears()
        {
            SkipIfNoData();

            var csvFiles = Directory.GetFiles(_dataPath, "*.csv").Take(10).ToList();
            var inconsistencies = new List<string>();

            foreach (var filePath in csvFiles)
            {
                var productCode = Path.GetFileNameWithoutExtension(filePath);
                var config = CreateConfig(productCode);
                var formsByYear = new Dictionary<int, HashSet<string>>();

                foreach (var line in File.ReadAllLines(filePath))
                {
                    var result = _instance.Reader(config, line, DateTime.UtcNow, false) as USDAFruitAndVegetable;
                    if (result == null) continue;

                    var year = result.Time.Year;
                    if (!formsByYear.TryGetValue(year, out var forms))
                    {
                        forms = new HashSet<string>();
                        formsByYear[year] = forms;
                    }
                    forms.Add(result.Form);
                }

                // Check for similar forms that differ only by case/punctuation
                var allForms = formsByYear.Values.SelectMany(f => f).Distinct().ToList();
                for (int i = 0; i < allForms.Count; i++)
                {
                    for (int j = i + 1; j < allForms.Count; j++)
                    {
                        if (AreSimilarForms(allForms[i], allForms[j]))
                        {
                            var years1 = formsByYear.Where(kvp => kvp.Value.Contains(allForms[i])).Select(kvp => kvp.Key);
                            var years2 = formsByYear.Where(kvp => kvp.Value.Contains(allForms[j])).Select(kvp => kvp.Key);
                            inconsistencies.Add(
                                $"{productCode}: '{allForms[i]}' (years: {string.Join(",", years1)}) " +
                                $"vs '{allForms[j]}' (years: {string.Join(",", years2)})"
                            );
                        }
                    }
                }
            }

            if (inconsistencies.Count > 0)
            {
                TestContext.WriteLine("Form name variations detected (may indicate data inconsistency):");
                foreach (var issue in inconsistencies)
                {
                    TestContext.WriteLine($"  - {issue}");
                }
                // This is a warning, not a failure - document the inconsistency
                Assert.Warn($"Found {inconsistencies.Count} form name variation(s) across years. " +
                    "This may cause algorithm filtering issues. See test output for details.");
            }
            else
            {
                TestContext.WriteLine("No form name inconsistencies detected");
            }
        }

        private static bool AreSimilarForms(string form1, string form2)
        {
            // Normalize: lowercase, replace punctuation with space, collapse whitespace
            var normalized1 = NormalizeForm(form1);
            var normalized2 = NormalizeForm(form2);

            // If normalized forms are identical but original forms differ, they're "similar but inconsistent"
            return normalized1 == normalized2 && form1 != form2;
        }

        private static string NormalizeForm(string form)
        {
            // Replace common punctuation variations with space
            var normalized = Regex.Replace(form.ToLowerInvariant(), @"[-;,]", " ");
            // Collapse multiple spaces
            normalized = Regex.Replace(normalized, @"\s+", " ").Trim();
            return normalized;
        }

        // === Schema Contract Tests ===

        [Test]
        public void AllCsvFilesHaveExactlyEightColumns()
        {
            SkipIfNoData();

            var csvFiles = Directory.GetFiles(_dataPath, "*.csv");
            var errors = new List<string>();

            foreach (var filePath in csvFiles)
            {
                var fileName = Path.GetFileName(filePath);
                var lineNumber = 0;

                foreach (var line in File.ReadAllLines(filePath))
                {
                    lineNumber++;
                    var columns = line.Split(',').Length;
                    if (columns != 8)
                    {
                        errors.Add($"{fileName}:{lineNumber} has {columns} columns (expected 8)");
                    }
                }
            }

            if (errors.Count > 0)
            {
                Assert.Fail($"Column count violations:\n{string.Join("\n", errors.Take(10))}");
            }

            TestContext.WriteLine($"Validated column count for {csvFiles.Length} files");
        }

        [Test]
        public void AllCsvFilesHaveValidDateFormat()
        {
            SkipIfNoData();

            var csvFiles = Directory.GetFiles(_dataPath, "*.csv");
            var errors = new List<string>();
            var datePattern = new Regex(@"^\d{8}$");

            foreach (var filePath in csvFiles)
            {
                var fileName = Path.GetFileName(filePath);
                var lineNumber = 0;

                foreach (var line in File.ReadAllLines(filePath))
                {
                    lineNumber++;
                    var firstColumn = line.Split(',').FirstOrDefault() ?? "";

                    if (!datePattern.IsMatch(firstColumn))
                    {
                        errors.Add($"{fileName}:{lineNumber} invalid date '{firstColumn}' (expected yyyyMMdd)");
                        continue;
                    }

                    // Validate it's a real date
                    if (!DateTime.TryParseExact(firstColumn, "yyyyMMdd", null,
                        System.Globalization.DateTimeStyles.None, out _))
                    {
                        errors.Add($"{fileName}:{lineNumber} invalid date '{firstColumn}' (not a valid date)");
                    }
                }
            }

            if (errors.Count > 0)
            {
                Assert.Fail($"Date format violations:\n{string.Join("\n", errors.Take(10))}");
            }

            TestContext.WriteLine($"Validated date format for {csvFiles.Length} files");
        }

        [Test]
        public void AllSymbolsHaveCorrespondingCsvFiles()
        {
            SkipIfNoData();

            // Get all symbol properties from Symbols class using reflection
            var symbolsType = typeof(USDAFruitAndVegetable.Symbols);
            var symbolProperties = symbolsType.GetProperties(BindingFlags.Public | BindingFlags.Static)
                .Where(p => p.PropertyType == typeof(string));

            var missingFiles = new List<string>();

            foreach (var prop in symbolProperties)
            {
                var productCode = (string)prop.GetValue(null)!;
                var expectedPath = Path.Combine(_dataPath, $"{productCode}.csv");

                if (!File.Exists(expectedPath))
                {
                    missingFiles.Add($"{prop.Name} ({productCode})");
                }
            }

            if (missingFiles.Count > 0)
            {
                Assert.Fail($"Missing CSV files for symbols:\n{string.Join("\n", missingFiles)}");
            }

            TestContext.WriteLine($"All {symbolProperties.Count()} symbol CSV files exist");
        }

        [Test]
        public void GetSourcePathMatchesDataFileLocation()
        {
            SkipIfNoData();

            var collection = new USDAFruitAndVegetables();
            var config = CreateConfig("apples");

            var source = collection.GetSource(config, DateTime.UtcNow, false);

            // The path should end with the expected structure
            Assert.That(source.Source, Does.EndWith(Path.Combine("alternative", "usda", "fruitandvegetables", "apples.csv")));

            // The Globals.DataFolder-based path should exist (when data is present)
            var expectedPath = Path.Combine(Globals.DataFolder, "alternative", "usda", "fruitandvegetables", "apples.csv");
            Assert.That(File.Exists(expectedPath), Is.True,
                $"Expected file at {expectedPath} but it doesn't exist. " +
                $"GetSource returned: {source.Source}");
        }

        // === Data Coverage Tests ===

        [Test]
        public void ReportsYearCoverageForProducts()
        {
            SkipIfNoData();

            var csvFiles = Directory.GetFiles(_dataPath, "*.csv").Take(10).ToList();
            var coverageReport = new Dictionary<string, List<int>>();

            foreach (var filePath in csvFiles)
            {
                var productCode = Path.GetFileNameWithoutExtension(filePath);
                var config = CreateConfig(productCode);
                var years = new HashSet<int>();

                foreach (var line in File.ReadAllLines(filePath))
                {
                    var result = _instance.Reader(config, line, DateTime.UtcNow, false) as USDAFruitAndVegetable;
                    if (result != null)
                    {
                        years.Add(result.Time.Year);
                    }
                }

                coverageReport[productCode] = years.OrderBy(y => y).ToList();
            }

            TestContext.WriteLine("Year coverage by product:");
            foreach (var kvp in coverageReport)
            {
                var years = kvp.Value;
                var gaps = FindYearGaps(years);
                var gapInfo = gaps.Count > 0 ? $" [gaps: {string.Join(", ", gaps)}]" : "";
                TestContext.WriteLine($"  {kvp.Key}: {string.Join(", ", years)}{gapInfo}");
            }

            Assert.That(coverageReport.Count, Is.GreaterThan(0), "No products found for coverage report");
        }

        [Test]
        public void ValidatesExampleAlgorithmDateRanges()
        {
            SkipIfNoData();

            // C# example uses 2021-01-01 to 2024-12-31
            // Python example uses 2018-01-01 to 2023-12-31
            var csStartYear = 2021;
            var csEndYear = 2024;
            var pyStartYear = 2018;
            var pyEndYear = 2023;

            var csvFiles = Directory.GetFiles(_dataPath, "*.csv");
            var productsWithCsData = new List<string>();
            var productsWithPyData = new List<string>();

            foreach (var filePath in csvFiles)
            {
                var productCode = Path.GetFileNameWithoutExtension(filePath);
                var config = CreateConfig(productCode);
                var years = new HashSet<int>();

                foreach (var line in File.ReadAllLines(filePath))
                {
                    var result = _instance.Reader(config, line, DateTime.UtcNow, false) as USDAFruitAndVegetable;
                    if (result != null)
                    {
                        years.Add(result.Time.Year);
                    }
                }

                // Check C# range overlap
                if (years.Any(y => y >= csStartYear && y <= csEndYear))
                {
                    productsWithCsData.Add(productCode);
                }

                // Check Python range overlap
                if (years.Any(y => y >= pyStartYear && y <= pyEndYear))
                {
                    productsWithPyData.Add(productCode);
                }
            }

            TestContext.WriteLine($"Products with data in C# example range ({csStartYear}-{csEndYear}): {productsWithCsData.Count}");
            TestContext.WriteLine($"Products with data in Python example range ({pyStartYear}-{pyEndYear}): {productsWithPyData.Count}");

            Assert.That(productsWithCsData, Is.Not.Empty,
                $"No products have data in C# example date range ({csStartYear}-{csEndYear})");
            Assert.That(productsWithPyData, Is.Not.Empty,
                $"No products have data in Python example date range ({pyStartYear}-{pyEndYear})");
        }

        private static List<string> FindYearGaps(List<int> sortedYears)
        {
            var gaps = new List<string>();
            for (int i = 1; i < sortedYears.Count; i++)
            {
                var gap = sortedYears[i] - sortedYears[i - 1];
                if (gap > 1)
                {
                    gaps.Add($"{sortedYears[i - 1]}-{sortedYears[i]} ({gap - 1} year gap)");
                }
            }
            return gaps;
        }
    }
}
