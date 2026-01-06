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

"""
CLRImports - Bridge module for Python-C# interoperability.

This module initializes the .NET CoreCLR runtime and imports essential
QuantConnect types for use in DataProcessing Python scripts.

Usage:
    from CLRImports import Config

Prerequisites:
    - clr-loader>=0.2.10 (supports .NET 10+)
    - pythonnet>=3.0.3
    - Must run from DataProcessing/bin/Debug/net10.0/ directory
      (contains process.runtimeconfig.json and QuantConnect.*.dll)

Note: The quantconnect-stubs package must NOT be installed as it shadows
the real CLR namespaces.
"""

from __future__ import annotations

from clr_loader import get_coreclr
from pythonnet import set_runtime

# Initialize .NET CoreCLR runtime.
# Expects to run from bin/Debug/net10.0/ where process.runtimeconfig.json lives.
# This follows the SDK template pattern where DataProcessing.csproj builds with
# AssemblyName=process, producing process.runtimeconfig.json in the output dir.
_runtime = get_coreclr(runtime_config="process.runtimeconfig.json")
set_runtime(_runtime)

# Now we can use clr to add assembly references
import clr  # noqa: E402

# Add references to the QuantConnect assemblies we need
clr.AddReference("QuantConnect.Common")
clr.AddReference("QuantConnect.Configuration")

# Export the types we need for DataProcessing
from QuantConnect.Configuration import Config  # noqa: E402, F401

__all__ = ["Config"]
