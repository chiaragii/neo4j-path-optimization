import matplotlib.pyplot as plt

def plot2d():
    y = [2.988961, 7.877312, 21.569412, 128.600391, 498.800449, 1555.826346]
    x = [223, 2385, 4859, 12409, 23902, 46452]

    plt.plot(x, y, label='Tempo di Caricamento in funzione del numero di nodi')

    plt.ylabel('Tempo di caricamento (secondi)')
    plt.xlabel('Numero di nodi totali')
    plt.title('Tempo di Caricamento in funzione dei nodi')

    # Aggiunta della legenda
    plt.legend()

    # Mostra il grafico
    plt.show()






def plot_data(cpu_values, memory_values, prova, file):
    # Plotting CPU and Memory data for each "Prova"
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8))

    ax1.plot(cpu_values, label='CPU Usage', color='blue')
    ax1.set_ylabel('CPU Usage (%)')
    ax1.set_title(file + ", prova: " + str(prova))
    ax1.grid(True)

    ax2.plot(memory_values, label='Memory Usage', color='red')
    ax2.set_xlabel('Time (seconds)')
    ax2.set_ylabel('Memory Usage (MB)')
    ax2.grid(True)

    plt.tight_layout()
    plt.savefig("data/output_files/cpu_monitor/"+file + ", prova: " + str(prova))

file_name = "memory_cpu_opt_4G"
file = "memory_cpu_opt_4G.txt"
# Reading data from file and plotting for each "Prova"
with open(file, "r") as file:
    prova = 1
    cpu_values = []
    memory_values = []
    for line in file:
        if line.startswith("Prova"):
            if cpu_values:  # Plot previous prova data
                plot_data(cpu_values, memory_values, prova, file_name)
                prova += 1
                cpu_values = []
                memory_values = []
        elif line.startswith("CPU"):
            cpu, memory, time = line.strip().split(" | ")
            cpu_values.append(float(cpu.split(": ")[1].strip("%")))
            memory_values.append(float(memory.split(": ")[1].strip(" MB")))

    # Plot last prova data
    plot_data(cpu_values, memory_values, prova, file_name)

