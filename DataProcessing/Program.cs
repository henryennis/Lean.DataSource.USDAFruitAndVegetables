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

namespace QuantConnect.DataProcessing;

/// <summary>
/// Stub entry point for DataProcessing.
/// <para>
/// Actual processing is implemented in Python (process.py).
/// This C# project exists to produce process.runtimeconfig.json,
/// which CLRImports.py needs to initialize the .NET CoreCLR runtime.
/// </para>
/// </summary>
public static class Program
{
    /// <summary>
    /// Entry point. Prints usage message and exits.
    /// </summary>
    /// <param name="args">Command line arguments (unused).</param>
    /// <returns>Exit code 0.</returns>
    public static int Main(string[] args)
    {
        Console.WriteLine("DataProcessing is implemented in Python.");
        Console.WriteLine("Run: python process.py --config ./config.json");
        return 0;
    }
}
