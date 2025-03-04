from Pyro4 import expose
import time


class Solver:
    def __init__(self, workers=None, input_file_name=None, output_file_name=None):
        self.input_file_name = input_file_name
        self.output_file_name = output_file_name
        self.workers = workers or []
        print("Initialized Solver")

    def solve(self):
        start_time = time.time()
        print('Job started')
        print("Workers %d" % len(self.workers))

        array = self.read_input()

        step = max(1, len(array) // len(self.workers))
        mapped = []
        for i in range(len(self.workers)):
            chunk = array[i * step: (i + 1) * step]
            mapped.append(self.workers[i].mymap(chunk))

        print('Map finished:', mapped)

        sorted_array = self.myreduce(mapped)
        print("Reduce finished:", sorted_array)

        elapsed_time = time.time() - start_time
        self.write_output(sorted_array, elapsed_time)
        print("Job Finished")

    @staticmethod
    @expose
    def mymap(arr):
        if len(arr) <= 1:
            return arr

        mid = len(arr) // 2
        left = Solver.mymap(arr[:mid])
        right = Solver.mymap(arr[mid:])
        return Solver.merge(left, right)

    @staticmethod
    @expose
    def myreduce(mapped):
        mapped = [entry.value for entry in mapped]
        while len(mapped) > 1:
            new_mapped = []
            for i in range(0, len(mapped), 2):
                if i + 1 < len(mapped):
                    new_mapped.append(Solver.merge(mapped[i], mapped[i + 1]))
                else:
                    new_mapped.append(mapped[i])
            mapped = new_mapped
        return mapped[0]

    @staticmethod
    def merge(left, right):
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

    def read_input(self):
        with open(self.input_file_name, 'r') as f:
            array = list(map(int, f.read().split()))
        return array

    def write_output(self, output, elapsed_time):
        f = open(self.output_file_name, 'w')
        f.write("Time: {}\n".format(elapsed_time))
        f.write('Sorted Array: ' + ' '.join(map(str, output)) + '\n')
        f.close()