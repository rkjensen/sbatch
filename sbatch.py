from subprocess import run

"""
To quickly generate a sbatch script in an iteractive manner. Made primarily when creating a bunch of different jobs with different parameters
Use GPU or CPU to initialize the script. Custom parameters for the can be passed through kwargs, and specifc lines can be written with .write_sbatch.
A list of lines (unalterd) can be written as .write_list([list_of_commands]). At the end if you want to submit you can run .submit_script()

Example usage:
script = GPU(filename='submit.sbatch',time=1:00,cpu=2,tasks=5, log='log_file.txt',mem=1024)
script.write_sbatch('-e error.txt')
cmd = ['module load GCC OpenMPI CUDA','source ~/.bashrc','python ~/some/script.py']
script.write_list(cmd)
script.submit_script()

"""

CPU_QUEUE = "htc-el8"
GPU_QUEUE = "gpu-el8"


class FileGenerator(object):
    def __init__(self, filename: str) -> None:
        self.filename = filename

    def write_line(self, line: str, mode: str = "a") -> None:
        with open(self.filename, mode) as f:
            f.write(f"{line}\n")

    def write_list(self, lines) -> None:
        if isinstance(lines, str):
            self.write_line(lines)
            return
        for line in lines:
            self.write_line(line)

    write_lines = write_list

    def submit_script(self):
        run(["sbatch", self.filename], check=True)


class SbatchGenerator(FileGenerator):
    def __init__(
        self,
        filename: str,
        time: str = "1-00:00:00",
        tasks: int = 1,
        cpu: int = 1,
        N: int = 1,
        log: str = None,
        error: str = None,
        array=None,
        array_throttle=None,
        **kwargs,
    ) -> None:
        super().__init__(filename)
        self.write_header()
        self.write_sbatch(f"--time={time}")
        self.write_sbatch(f"--tasks={tasks}")
        self.write_sbatch(f"--cpus-per-task={cpu}")
        self.write_sbatch(f"-N {N}")
        log, error = self.check_log_error(log, error, array, N)
        self.write_sbatch(f"--output {log}")
        self.write_sbatch(f"--error {error}")
        if array:
            if array_throttle:
                self.write_sbatch(f'--array={array}%{array_throttle}')
            else:
                self.write_sbatch(f"--array={array}")
        for key, value in kwargs.items():
            self.write_sbatch(f"--{key}={value}")

    def check_log_error(self, log, error, array, N):
        log = self.check_log(log, array, N)
        error = self.check_error(error, array, N)
        return log, error

    def check_error(self, error, array, N):
        if not error and not array and N == 1:
            error = f"slurm-%j-%N.err.txt"
        if not error and not array and N > 1:
            error = f"slurm-%j.err.txt"
        if not error and array and N == 1:
            error = f"slurm-%A_%a_%N.err.txt"
        if not error and array and N > 1:
            error = f"slurm-%A_%a.err.txt"
        return error

    def check_log(self, log, array, N):
        if not log and not array and N == 1:
            log = f"slurm-%j-%N.out.txt"
        if not log and not array and N > 1:
            log = f"slurm-%j.out.txt"
        if not log and array and N == 1:
            log = f"slurm-%A_%a_%N.out.txt"
        if not log and array and N > 1:
            log = f"slurm-%A_%a.out.txt"
        return log

    def write_sbatch(self, line: str) -> None:
        self.write_line(f"#SBATCH {line}")

    def write_header(self) -> None:
        self.write_line("#!/bin/bash", mode="w")

    def submit_script(self, **kwargs) -> None:
        run(
            f"sbatch {self.filename}",
            shell=True,
            **kwargs,
        )
class NewSbatchGenerator(FileGenerator):
    def __init__(
        self,
        filename: str,
        time: str = "1-00:00:00",
        tasks: int = 1,
        cpu: int = 1,
        N: int = 1,
        log: str = None,
        error: str = None,
        array=None,
        array_throttle=None,
        **kwargs,
    ) -> None:
        super().__init__(filename)
        self.write_header()
        self.write_sbatch(f"--time={time}")
        self.write_sbatch(f"--tasks={tasks}")
        self.write_sbatch(f"--cpus-per-task={cpu}")
        #self.write_sbatch(f"-N {N}")
        log, error = self.check_log_error(log, error, array, N)
        self.write_sbatch(f"--output {log}")
        self.write_sbatch(f"--error {error}")
        if array:
            if array_throttle:
                self.write_sbatch(f'--array={array}%{array_throttle}')
            else:
                self.write_sbatch(f"--array={array}")
        for key, value in kwargs.items():
            self.write_sbatch(f"--{key}={value}")

    def check_log_error(self, log, error, array, N):
        log = self.check_log(log, array, N)
        error = self.check_error(error, array, N)
        return log, error

    def check_error(self, error, array, N):
        if not error and not array and N == 1:
            error = f"slurm-%j-%N.err.txt"
        if not error and not array and N > 1:
            error = f"slurm-%j.err.txt"
        if not error and array and N == 1:
            error = f"slurm-%A_%a_%N.err.txt"
        if not error and array and N > 1:
            error = f"slurm-%A_%a.err.txt"
        return error

    def check_log(self, log, array, N):
        if not log and not array and N == 1:
            log = f"slurm-%j-%N.out.txt"
        if not log and not array and N > 1:
            log = f"slurm-%j.out.txt"
        if not log and array and N == 1:
            log = f"slurm-%A_%a_%N.out.txt"
        if not log and array and N > 1:
            log = f"slurm-%A_%a.out.txt"
        return log

    def write_sbatch(self, line: str) -> None:
        self.write_line(f"#SBATCH {line}")

    def write_header(self) -> None:
        self.write_line("#!/bin/bash", mode="w")

    def submit_script(self, **kwargs) -> None:
        run(
            f"sbatch {self.filename}",
            shell=True,
            **kwargs,
        )


class SbatchGeneratorCPU(SbatchGenerator):
    def __init__(
        self,
        filename: str,
        time: str = "1-00:00",
        tasks: int = 1,
        cpu: int = 1,
        N: int = 1,
        log: str = None,
        error: str = None,
        array=None,
        **kwargs,
    ) -> None:
        super().__init__(filename, time, tasks, cpu, N, log, error, array, **kwargs)
        self.write_sbatch(f"-p {CPU_QUEUE}")


class SbatchGeneratorGPU(SbatchGenerator):
    def __init__(
        self,
        filename: str,
        time: str = "1-00:00",
        tasks: int = 1,
        cpu: int = 1,
        gpu: int = 1,
        N: int = 1,
        log: str = None,
        error: str = None,
        array=None,
        **kwargs,
    ) -> None:
        super().__init__(filename, time, tasks, cpu, N, log, error, array, **kwargs)
        self.write_sbatch(f"--gres=gpu:{gpu}")
        self.write_sbatch(f"-p {GPU_QUEUE}")


class SbatchGeneratorA100(SbatchGeneratorGPU):
    def __init__(self, filename: str, time: str = "7-00:00", gpu=1, **kwargs):
        super().__init__(filename, time, cpu=14, tasks=1, N=1, mem="128789", gpu=gpu, **kwargs)
        self.write_sbatch("-C gpu=A100")


class SbatchGeneratorA40(SbatchGeneratorGPU):
    def __init__(self, filename: str, time: str = "7-00:00", **kwargs):
        super().__init__(filename, time, cpu=64, tasks=1, N=1, mem="514286", **kwargs)
        self.write_sbatch("-C gpu=A40")
    
class SbatchGenerator3090(SbatchGenerator):
    def __init__(self,filename: str, time: str = "1-00:00", tasks: int = 1, cpu: int = 16, N: int = 1, log: str = None, error: str = None, array=None, **kwargs):
        super().__init__(filename, time, tasks, cpu, N, log, error, array, **kwargs)
        self.write_sbatch(f'--gres=gpu:3090:1')
        self.write_sbatch(f'--mem-per-gpu=64283')
        self.write_sbatch(f'-p {GPU_QUEUE}')
        #self.write_sbatch(f'-C rome,gpu=3090')

class SbatchGeneratormaxL40(NewSbatchGenerator):
    def __init__(self, filename: str, time: str = "1-00:00:00", tasks: int = 1, cpu: int = 4, N: int = 1, log: str = None, error: str = None, array=None, mem_per_gpu: int = 48300, **kwargs):
        super().__init__(filename,time,tasks,cpu,N,log,error,array,**kwargs)
        self.write_sbatch(f'--mem-per-gpu={mem_per_gpu}')
        self.write_sbatch(f'-C genoa,gpu=L40s')
#        self.write_sbatch(f'--gres=shard:1')
        self.write_sbatch(f'-p {GPU_QUEUE}')
        self.write_sbatch(f'-G 1')

class SbatchGeneratorMiniGPU(NewSbatchGenerator):
    def __init__(self, filename: str, time: str = "1-00:00:00", tasks: int = 1, cpu: int = 4, N: int = 1, log: str = None, error: str = None, array=None, mem: int = '20G', **kwargs):
        super().__init__(filename,time,tasks,cpu,N,log,error,array,**kwargs)
#        self.write_sbatch(f'--gres=shard:1')
        self.write_sbatch(f'-p {GPU_QUEUE}')
        self.write_sbatch(f'-G 1')
        self.write_sbatch(f'--mem {mem}')



GPU = SbatchGeneratorGPU
CPU = SbatchGeneratorCPU
SBATCH = SbatchGenerator
A100 = SbatchGeneratorA100
A40 = SbatchGeneratorA40
GPU3090 = SbatchGenerator3090
maxL40=SbatchGeneratormaxL40
templateGPU=SbatchGeneratorMiniGPU


if __name__ == "__main__":
    print("sbatch.py is a module and is not supposed to be called directly")
