import random


def generate_random_array(length, filename):
    random_array = [random.randint(1, 1000) for _ in range(length)]

    with open(filename, 'w') as f:
        f.write(" ".join(map(str, random_array)) + "\n")

    print(f"Array of length {length} saved to {filename}")

if __name__ == "__main__":
    length = 2000000
    generate_random_array(length, f"input/{length}_array.txt")
