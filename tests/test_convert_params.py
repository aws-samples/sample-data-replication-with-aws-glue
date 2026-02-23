"""Tests for infrastructure/scripts/convert_params.py"""

import json
import os
import subprocess
import sys
import tempfile

import pytest

# Add the scripts directory to the path so we can import the module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "infrastructure", "scripts"))
from convert_params import cfn_array_to_flat, detect_format, flat_to_cfn_array


# --- detect_format ---

class TestDetectFormat:
    def test_detects_cfn_array(self):
        data = [{"ParameterKey": "K", "ParameterValue": "V"}]
        assert detect_format(data) == "cfn_array"

    def test_detects_flat_json(self):
        data = {"JobName": "my-job"}
        assert detect_format(data) == "flat_json"

    def test_detects_empty_list_as_cfn_array(self):
        assert detect_format([]) == "cfn_array"

    def test_detects_empty_dict_as_flat_json(self):
        assert detect_format({}) == "flat_json"

    def test_raises_on_invalid_type(self):
        with pytest.raises(ValueError, match="Unsupported JSON root type"):
            detect_format("not a dict or list")

    def test_raises_on_none(self):
        with pytest.raises(ValueError):
            detect_format(None)


# --- flat_to_cfn_array ---

class TestFlatToCfnArray:
    def test_basic_conversion(self):
        flat = {"JobName": "my-job", "SourceEngineType": "sqlserver"}
        result = flat_to_cfn_array(flat)
        assert result == [
            {"ParameterKey": "JobName", "ParameterValue": "my-job"},
            {"ParameterKey": "SourceEngineType", "ParameterValue": "sqlserver"},
        ]

    def test_empty_dict(self):
        assert flat_to_cfn_array({}) == []

    def test_non_string_values_coerced(self):
        flat = {"Workers": 3, "Enabled": True}
        result = flat_to_cfn_array(flat)
        assert result == [
            {"ParameterKey": "Workers", "ParameterValue": "3"},
            {"ParameterKey": "Enabled", "ParameterValue": "True"},
        ]

    def test_single_parameter(self):
        result = flat_to_cfn_array({"Key": "Val"})
        assert len(result) == 1
        assert result[0] == {"ParameterKey": "Key", "ParameterValue": "Val"}


# --- cfn_array_to_flat ---

class TestCfnArrayToFlat:
    def test_basic_conversion(self):
        cfn = [
            {"ParameterKey": "JobName", "ParameterValue": "my-job"},
            {"ParameterKey": "SourceEngineType", "ParameterValue": "sqlserver"},
        ]
        result = cfn_array_to_flat(cfn)
        assert result == {"JobName": "my-job", "SourceEngineType": "sqlserver"}

    def test_empty_list(self):
        assert cfn_array_to_flat([]) == {}

    def test_duplicate_keys_last_wins(self):
        cfn = [
            {"ParameterKey": "Key", "ParameterValue": "first"},
            {"ParameterKey": "Key", "ParameterValue": "second"},
        ]
        result = cfn_array_to_flat(cfn)
        assert result == {"Key": "second"}

    def test_non_string_values_coerced(self):
        cfn = [{"ParameterKey": "Count", "ParameterValue": 42}]
        result = cfn_array_to_flat(cfn)
        assert result == {"Count": "42"}


# --- CLI integration ---

class TestCLI:
    SCRIPT = os.path.join(
        os.path.dirname(__file__), "..", "infrastructure", "scripts", "convert_params.py"
    )

    def _run(self, *args):
        return subprocess.run(
            [sys.executable, self.SCRIPT, *args],
            capture_output=True,
            text=True,
        )

    def test_detect_flat_json(self, tmp_path):
        f = tmp_path / "input.json"
        f.write_text(json.dumps({"JobName": "test"}))
        result = self._run(str(f), "--detect")
        assert result.returncode == 0
        assert result.stdout.strip() == "flat_json"

    def test_detect_cfn_array(self, tmp_path):
        f = tmp_path / "input.json"
        f.write_text(json.dumps([{"ParameterKey": "K", "ParameterValue": "V"}]))
        result = self._run(str(f), "--detect")
        assert result.returncode == 0
        assert result.stdout.strip() == "cfn_array"

    def test_detect_is_default_mode(self, tmp_path):
        f = tmp_path / "input.json"
        f.write_text(json.dumps({"A": "1"}))
        result = self._run(str(f))
        assert result.returncode == 0
        assert result.stdout.strip() == "flat_json"

    def test_to_cfn_conversion(self, tmp_path):
        inp = tmp_path / "input.json"
        out = tmp_path / "output.json"
        inp.write_text(json.dumps({"JobName": "test", "Workers": "3"}))
        result = self._run(str(inp), str(out), "--to-cfn")
        assert result.returncode == 0
        output_data = json.loads(out.read_text())
        assert output_data == [
            {"ParameterKey": "JobName", "ParameterValue": "test"},
            {"ParameterKey": "Workers", "ParameterValue": "3"},
        ]

    def test_to_flat_conversion(self, tmp_path):
        inp = tmp_path / "input.json"
        out = tmp_path / "output.json"
        inp.write_text(json.dumps([
            {"ParameterKey": "JobName", "ParameterValue": "test"},
        ]))
        result = self._run(str(inp), str(out), "--to-flat")
        assert result.returncode == 0
        output_data = json.loads(out.read_text())
        assert output_data == {"JobName": "test"}

    def test_invalid_json_exits_nonzero(self, tmp_path):
        f = tmp_path / "bad.json"
        f.write_text("not json at all")
        result = self._run(str(f), "--detect")
        assert result.returncode != 0
        assert "invalid JSON" in result.stderr

    def test_empty_file_exits_nonzero(self, tmp_path):
        f = tmp_path / "empty.json"
        f.write_text("")
        result = self._run(str(f), "--detect")
        assert result.returncode != 0
        assert "empty file" in result.stderr

    def test_to_cfn_rejects_cfn_input(self, tmp_path):
        f = tmp_path / "input.json"
        out = tmp_path / "output.json"
        f.write_text(json.dumps([{"ParameterKey": "K", "ParameterValue": "V"}]))
        result = self._run(str(f), str(out), "--to-cfn")
        assert result.returncode != 0
        assert "already CFN array" in result.stderr

    def test_to_flat_rejects_flat_input(self, tmp_path):
        f = tmp_path / "input.json"
        out = tmp_path / "output.json"
        f.write_text(json.dumps({"K": "V"}))
        result = self._run(str(f), str(out), "--to-flat")
        assert result.returncode != 0
        assert "already flat JSON" in result.stderr

    def test_file_not_found(self):
        result = self._run("/nonexistent/path.json", "--detect")
        assert result.returncode != 0
        assert "file not found" in result.stderr
