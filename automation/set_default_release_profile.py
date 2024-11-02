import sys
import toml
from pathlib import Path
import copy
def set_default_release_profile(profile_type: str):    
    # Compute project root path (2 levels up from script location)
    script_path = Path(__file__).resolve()
    project_root = script_path.parent.parent
    cargo_path = project_root / 'Cargo.toml'
    
    if not cargo_path.exists():
        print(f"Error: Cargo.toml not found at {cargo_path}")
        sys.exit(1)
    
    # Parse toml file
    with open(cargo_path) as f:
        cargo_config = toml.load(f)
    
    # Get source profile
    source_profile = f'release-{profile_type}'
    if source_profile not in cargo_config['profile']:
        print(f"Error: profile.{source_profile} not found in Cargo.toml")
        sys.exit(1)
    
    # Replace release profile with selected profile
    cargo_config['profile']['release'] = copy.deepcopy(cargo_config['profile'][source_profile])
    del cargo_config['profile']["release"]["inherits"]
    
    # Write updated config back to file
    with open(cargo_path, 'w') as f:
        toml.dump(cargo_config, f)
    
    print(f"Successfully updated release profile with {source_profile} configuration")

if __name__ == "__main__":
    # set_default_release_profile('opt')

    if len(sys.argv) != 2:
        print("Usage: python set_default_release_profile.py <dev|opt|small>")
        sys.exit(1)
    
    set_default_release_profile(sys.argv[1])

