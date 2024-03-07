from numba import cuda
import numpy as np
import time

# Defina uma função para executar na GPU
@cuda.jit
def add_arrays_gpu(a, b, result):
    idx = cuda.grid(1)
    if idx < a.size:
        result[idx] = a[idx] + b[idx]

def main():
    # Inicialize os arrays de dados
    n = 100000
    a = np.random.rand(n).astype(np.float32)
    b = np.random.rand(n).astype(np.float32)
    result_gpu = np.zeros_like(a)

    # Calcule o número de threads e blocos
    threads_per_block = 256
    blocks_per_grid = (a.size + (threads_per_block - 1)) // threads_per_block

    start_time = time.time()
    # Execute a função na GPU
    add_arrays_gpu[blocks_per_grid, threads_per_block](a, b, result_gpu)
    cuda.synchronize()  # Aguarde a conclusão da execução
    end_time = time.time()

    print(f"Resultado GPU: {result_gpu[:5]}...")  # Mostre apenas os primeiros 5 resultados para brevidade
    print(f"Tempo de execução na GPU: {end_time - start_time} segundos")

if __name__ == "__main__":
    main()
