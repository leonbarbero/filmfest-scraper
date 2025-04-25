# src/storage.py

import json
import os
import tempfile

def save_record(record: dict, output_path: str):
    """
    Append a single record (as JSON) to a JSONL file at output_path.
    """
    directory = os.path.dirname(output_path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(output_path, 'a', encoding='utf-8') as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

def save_state(state: dict, state_path: str):
    """
    Overwrite a JSON file at state_path with the current crawler state.
    """
    directory = os.path.dirname(state_path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(state_path, 'w', encoding='utf-8') as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def load_state(state_path: str) -> dict:
    """
    Load crawler state from a JSON file, or return empty dict if it doesn't exist.
    """
    if not os.path.exists(state_path):
        return {}
    with open(state_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def smoke_test():
    """
    Quick check that save_record, save_state, and load_state work correctly.
    """
    print("  ▶ Running storage.smoke_test()…")
    # Create a temp directory
    tmpdir = tempfile.mkdtemp()

    # 1) Test record saving
    rec = {'festival': 'Test Fest', 'deadline': '2025-12-31'}
    out_path = os.path.join(tmpdir, 'test_data.jsonl')
    save_record(rec, out_path)
    assert os.path.exists(out_path), "JSONL file was not created"
    with open(out_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    assert len(lines) == 1, f"Expected 1 line, got {len(lines)}"
    loaded = json.loads(lines[0])
    assert loaded == rec, f"Record mismatch: {loaded} vs {rec}"

    # 2) Test state save & load
    state = {'queued': ['a','b'], 'done': ['c']}
    state_path = os.path.join(tmpdir, 'state.json')
    save_state(state, state_path)
    loaded_state = load_state(state_path)
    assert loaded_state == state, f"State mismatch: {loaded_state} vs {state}"

    print("  ✓ Storage module smoke test passed")
