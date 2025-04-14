import yaml
import argparse

def load_yaml(file_path):
    """Load YAML file."""
    with open(file_path, "r") as f:
        return yaml.safe_load(f)

def extract_pvs(yaml_data, filter_text=None):
    """Extract PV names, filtering out invalid ones and applying optional status filtering."""
    pvs = []
    for pv_entries in yaml_data.values():
        if isinstance(pv_entries, list):
            for entry in pv_entries:
                if isinstance(entry, dict):
                    pv, status = list(entry.items())[0]  # Extract PV and its status
                    if '?' not in pv and (filter_text is None or filter_text in status):
                        pvs.append(pv)
    return pvs

def write_pvs_to_file(pvs, output_file):
    """Write PVs to a text file, one per line."""
    with open(output_file, "w") as f:
        for pv in pvs:
            f.write(f"{pv}\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract and filter PVs from a YAML file.")
    parser.add_argument("-f", "--yaml_file", help="Path to the YAML file")
    parser.add_argument("-o", "--output_file", help="Path to the output text file")
    parser.add_argument("--filter", choices=["Paused", "Archived"], help="Filter PVs by status")

    args = parser.parse_args()

    yaml_data = load_yaml(args.yaml_file)
    if args.filter =='Paused':
        filter = 'Paused'
    elif args.filter == 'Archived':
        filter= "Not being archived"
    pvs = extract_pvs(yaml_data, filter)
    write_pvs_to_file(pvs, args.output_file)

    print(f"Filtered PV list written to {args.output_file}")
