import time
import multiprocessing as mp


class Solver:
    def __init__(self, array, workers):
        self.array = array
        self.workers = workers

    def merge_sort(self, chunk):
        if len(chunk) <= 1:
            return chunk
        mid = len(chunk) // 2
        left = self.merge_sort(chunk[:mid])
        right = self.merge_sort(chunk[mid:])
        return self.merge(left, right)

    def merge(self, left, right):
        result = []
        i = j = 0
        while i < len(left) and j < len(right):
            if left[i] < right[j]:
                result.append(left[i])
                i += 1
            else:
                result.append(right[j])
                j += 1

        result.extend(left[i:])
        result.extend(right[j:])
        return result

    def parallel(self):
        print("Parallel Merge Sort started...")

        start_time = time.time()

        pool = mp.Pool(processes=self.workers)
        chunk_size = len(self.array) // self.workers
        ranges = [self.array[i * chunk_size:min((i + 1) * chunk_size, len(self.array))] for i in range(self.workers)]

        sorted_chunks = pool.map(self.merge_sort, ranges)
        pool.close()
        pool.join()

        while len(sorted_chunks) > 1:
            new_chunks = []
            for i in range(0, len(sorted_chunks), 2):
                if i + 1 < len(sorted_chunks):
                    new_chunks.append(self.merge(sorted_chunks[i], sorted_chunks[i + 1]))
                else:
                    new_chunks.append(sorted_chunks[i])
            sorted_chunks = new_chunks

        print("Parallel Merge Sort finished\n")
        elapsed_time = time.time() - start_time
        return sorted_chunks[0], elapsed_time

    def sequential(self):
        print("Sequential Merge Sort started...")

        start_time = time.time()

        sorted_array = self.merge_sort(self.array)

        print("Sequential Merge Sort finished\n")
        elapsed_time = time.time() - start_time
        return sorted_array, elapsed_time

def read_array_from_file(filepath):
    with open(filepath, 'r') as f:
        array = list(map(int, f.read().split()))
    return array


if __name__ == "__main__":
    input_file = "input/2000000_array.txt"
    array = read_array_from_file(input_file)
    solver = Solver(array, workers=2)

    sorted_array_parallel, time_parallel = solver.parallel()
    print(f"Time (Parallel): {time_parallel} seconds")

    sorted_array_sequential, time_sequential = solver.sequential()
    print(f"Time (Sequential): {time_sequential} seconds")
