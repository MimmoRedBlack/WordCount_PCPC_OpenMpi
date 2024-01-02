import matplotlib.pyplot as plt

# Dati
slave = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]
speed_up = [1, 1.33, 1.86, 2.5, 3.1, 3.15, 4.05, 4.79, 5.12, 5.44, 6.12, 6.71, 8.26, 8.5, 8.84, 9.39, 9.91, 9.97, 9.71, 9.83, 11.25, 12.95, 14.12]
execution_time = [34.203, 25.721, 18.433, 13.689, 11.023, 10.874, 8.433, 7.117, 6.681, 6.295, 5.592, 5.093, 4.139, 4.021, 3.872, 3.643, 3.453, 3.434, 3.522, 3.487, 3.043, 2.639, 2.421]

# Grafico a linee
fig, ax1 = plt.subplots(figsize=(10, 6))

# Plot di Speed-Up
ax1.set_xlabel('Numero di Slave', fontsize=12)
ax1.set_ylabel('Speed-Up', color='tab:blue', fontsize=12)
ax1.plot(slave, speed_up, color='tab:blue', marker='o', label='Speed-Up')
ax1.tick_params(axis='y', labelcolor='tab:blue')
ax1.legend(loc='upper left')

ax2 = ax1.twinx()
# Plot di Tempo di Esecuzione
ax2.set_ylabel('Tempo di Esecuzione (s)', color='tab:red', fontsize=12)
ax2.plot(slave, execution_time, color='tab:red', marker='x', label='Tempo di Esecuzione')
ax2.tick_params(axis='y', labelcolor='tab:red')

# Aggiungi griglia e titolo
ax1.grid(True, linestyle='--', alpha=0.7)
ax1.set_title('Andamento di Speed-Up e Tempo di Esecuzione al Variare del Numero di Slave', fontsize=14)

fig.tight_layout()
plt.show()
