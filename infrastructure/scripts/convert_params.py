#!/usr/bin/env python3
"""
Parameter format converter for AWS Glue Data Replication.

Converts between flat JSON format ({"key": "value"}) and CloudFormation
array format ([{"ParameterKey": "key", "ParameterValue": "value"}]).

Usage:
    python3 convert_params.py <input_file> <output_file> [--to-cfn|--to-flat|--detect]

Modes:
    --detect  (default) Print detected format to stdout ("cfn_array" or "flat_json")
    --to-cfn  Convert flat JSON to CFN array format, write to output_file
    --to-flat Convert CFN array to flat JSON, write to output_file
"""

import json
import sys


def detect_format(data):
    """Detect the parameter file format.

    Args:
        data: Parsed JSON data (list or dict).

    Returns:
        "cfn_array" if data is a list, "flat_json" if data is a dict.

    Raises:
        ValueError: If data is neither a list nor a dict.
    """
    if isinstance(data, list):
        return "cfn_array"
    if isinstance(data, dict):
        return "flat_json"
    raise ValueError(f"Unsupported JSON root type: {type(data).__name__}")


def flat_to_cfn_array(flat_dict):
    """Convert flat JSON dict to CFN array format.

    Args:
        flat_dict: Dict of {"key": "value"} pairs.

    Returns:
        List of {"ParameterKey": key, "ParameterValue": str(value)} dicts.
    """
    return [
        {"ParameterKey": k, "ParameterValue": str(v)}
        for k, v in flat_dict.items()
    ]


def cfn_array_to_flat(cfn_list):
    """Convert CFN array format to flat JSON dict.

    Duplicate keys: last value wins.

    Args:
        cfn_list: List of {"ParameterKey": ..., "ParameterValue": ...} dicts.

    Returns:
        Dict of {"key": "value"} pairs.
    """
    result = {}
    for entry in cfn_list:
        key = entry["ParameterKey"]
        value = str(entry["ParameterValue"])
        result[key] = value
    return result


def main():
    if len(sys.argv) < 2:
        print("Usage: convert_params.py <input_file> [output_file] [--to-cfn|--to-flat|--detect]", file=sys.stderr)
        sys.exit(1)

    # Separate flags from positional arguments
    mode = "detect"
    positional = []
    for arg in sys.argv[1:]:
        if arg == "--to-cfn":
            mode = "to_cfn"
        elif arg == "--to-flat":
            mode = "to_flat"
        elif arg == "--detect":
            mode = "detect"
        else:
            positional.append(arg)

    if not positional:
        print("Usage: convert_params.py [--to-cfn|--to-flat|--detect] <input_file> [output_file]", file=sys.stderr)
        sys.exit(1)

    input_file = positional[0]
    output_file = positional[1] if len(positional) > 1 else None

    # Read and parse input
    try:
        with open(input_file) as f:
            content = f.read().strip()
            if not content:
                print("Error: empty file", file=sys.stderr)
                sys.exit(1)
            data = json.loads(content)
    except json.JSONDecodeError as e:
        print(f"Error: invalid JSON — {e}", file=sys.stderr)
        sys.exit(1)
    except FileNotFoundError:
        print(f"Error: file not found — {input_file}", file=sys.stderr)
        sys.exit(1)

    fmt = detect_format(data)

    if mode == "detect":
        print(fmt)
        sys.exit(0)

    if mode == "to_cfn":
        if fmt != "flat_json":
            print("Error: input is already CFN array format", file=sys.stderr)
            sys.exit(1)
        if not output_file:
            print("Error: output file required for --to-cfn", file=sys.stderr)
            sys.exit(1)
        result = flat_to_cfn_array(data)
        with open(output_file, "w") as f:
            json.dump(result, f, indent=2)

    elif mode == "to_flat":
        if fmt != "cfn_array":
            print("Error: input is already flat JSON format", file=sys.stderr)
            sys.exit(1)
        if not output_file:
            print("Error: output file required for --to-flat", file=sys.stderr)
            sys.exit(1)
        result = cfn_array_to_flat(data)
        with open(output_file, "w") as f:
            json.dump(result, f, indent=2)


if __name__ == "__main__":
    main()
