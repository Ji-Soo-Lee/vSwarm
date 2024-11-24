import argparse
import time

def memory_stress(memory_mb, duration_ms):
    # Allocate the specified memory in MB
    allocated_memory = bytearray(memory_mb * 1024 * 1024)
    print(f"Allocated {memory_mb} MB of memory.")
    time.sleep(duration_ms / 1000)  # Convert milliseconds to seconds
    print(f"Memory stress completed for {duration_ms} milliseconds.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Memory Stress Script")
    parser.add_argument("-m", "--memory", type=int, required=True, help="Memory load in MB")
    parser.add_argument("-t", "--time", type=int, required=True, help="Duration in milliseconds")
    args = parser.parse_args()
    
    memory_stress(args.memory, args.time)
