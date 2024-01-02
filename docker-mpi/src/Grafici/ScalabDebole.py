import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

# Dati
slave = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]
total_size = [25, 50, 75, 100, 125, 150, 175, 200, 225, 250, 275, 300, 325, 350, 375, 400, 425, 450, 475, 500, 525, 550, 575]
execution_time = [1.617, 1.932, 2.133, 2.156, 2.212, 2.228, 2.238, 2.230, 2.249, 2.292, 2.311, 2.338, 2.349, 2.341, 2.352, 2.366, 2.383, 2.424, 2.417, 2.438, 2.454, 2.410, 2.381]

# Creazione del grafico
fig = plt.figure(figsize=(10, 8))
ax = fig.add_subplot(111, projection='3d')

# Grafico a dispersione
scatter = ax.scatter(slave, total_size, execution_time, c=execution_time, cmap='viridis', marker='o')

# Aggiunta delle etichette
ax.set_xlabel('Numero di Slave')
ax.set_ylabel('Total Size (MB)')
ax.set_zlabel('Tempo di Esecuzione (s)')
ax.set_title('Scalabilità Debole - Tempo di Esecuzione al Variare di Slave e TotalSize')

# Aggiunta della barra del colore
fig.colorbar(scatter, label='Tempo di Esecuzione (s)')

# Mostra il grafico
plt.show()
