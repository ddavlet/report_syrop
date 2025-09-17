import os
import time

OUT_DIR = './out'
DAYS_OLD = 60
now = time.time()
cutoff = now - DAYS_OLD * 86400

def clean_old_files(directory):
    """Recursively clean old files in directory and subdirectories"""
    if not os.path.isdir(directory):
        return

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
            clean_old_files(filepath)

# Clean files in OUT_DIR and all subdirectories
clean_old_files(OUT_DIR)
