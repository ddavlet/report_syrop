import os
import time
import argparse

OUT_DIR = './out'

def clean_old_files(directory, days_old):
    """Recursively clean old files in directory and subdirectories"""
    if not os.path.isdir(directory):
        return

    now = time.time()
    cutoff = now - days_old * 86400

    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)

        if os.path.isfile(filepath):
            file_mtime = os.path.getmtime(filepath)
            if file_mtime < cutoff:
                try:
                    os.remove(filepath)
                    print(f"Deleted: {filepath}")
                except Exception as e:
                    print(f"Error deleting {filepath}: {e}")
        elif os.path.isdir(filepath):
            # Recursively process subdirectories
            clean_old_files(filepath, days_old)

def main():
    parser = argparse.ArgumentParser(description='Clean old files from output directory')
    parser.add_argument('-D', '--days', type=int, default=60,
                       help='Number of days old files should be to delete (default: 60)')

    args = parser.parse_args()

    print(f"Cleaning files older than {args.days} days from {OUT_DIR}")
    clean_old_files(OUT_DIR, args.days)

if __name__ == "__main__":
    main()
