import sys

if __name__ == "__main__":
    print(f"Arguments count: {len(sys.argv)}")
    for i, arg in enumerate(sys.argv):
        print(f"Argument {i:>6}: {arg}")


# arg = sys.argv[1]
# print(arg[::-1])
# run python reverse.py "Real Python"