import re
import sys
from pathlib import Path

if __name__ == "__main__":
    directory = Path(sys.argv[1])
    file_pattern = re.compile(sys.argv[2])
    matches = [f for f in directory.iterdir() if file_pattern.match(f.name)]
    print(str(matches[0]))

