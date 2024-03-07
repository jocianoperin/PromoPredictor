import numpy as np
import time

def add_arrays_cpu(a, b, result):
    # Adição de arrays na CPU usando NumPy
    result[:] = np.add(a, b)

def main():
    # Inicialize os arrays de dados
    n = 100000
    a = np.random.rand(n).astype(np.float32)
    b = np.random.rand(n).astype(np.float32)
    result_cpu = np.zeros_like(a)

    start_time = time.time()
    # Execute a função na CPU
    add_arrays_cpu(a, b, result_cpu)
    end_time = time.time()

    print(f"Resultado CPU: {result_cpu[:5]}...")  # Mostre apenas os primeiros 5 resultados para brevidade
    print(f"Tempo de execução na CPU: {end_time - start_time} segundos")

if __name__ == "__main__":
    main()
