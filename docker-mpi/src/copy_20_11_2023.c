#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>
#include <mpi.h>
#include <ctype.h>

#define MASTER_RANK 0
#define INPUT_DIR "./InputFile"
#define MAX_WORD_LENGTH 50 // Sostituisci con il valore appropriato

// Struttura per rappresentare un elemento nella lista di trabocco
typedef struct OccurrenceNode
{
    int count;
    char word[MAX_WORD_LENGTH];
    struct OccurrenceNode *next;
} OccurrenceNode;

// Struttura per rappresentare la hashtable
typedef struct
{
    unsigned int size;
    OccurrenceNode **table;
} Hashtable;

// Dichiarazione della hashtable globale
Hashtable globalHashtable;

// Dichiarazione della struttura MPI_OCCURRENCE_NODE
MPI_Datatype MPI_OCCURRENCE_NODE;

// Funzione per inizializzare la hashtable con una dimensione specifica
void initializeHashtable(unsigned int size)
{
    globalHashtable.size = size;

    // Alloca dinamicamente la memoria per la tabella hash
    globalHashtable.table = (OccurrenceNode **)malloc(size * sizeof(OccurrenceNode *));

    // Inizializza tutti gli elementi della tabella hash a NULL
    for (unsigned int i = 0; i < size; ++i)
    {
        globalHashtable.table[i] = NULL;
    }
}

OccurrenceNode *all_occurrences = NULL;
OccurrenceNode *local_occurrences;

// Funzione per deallocare la memoria della hashtable
void freeHashtable()
{
    // Dealloca le liste di trabocco e la tabella hash stessa
    for (unsigned int i = 0; i < globalHashtable.size; ++i)
    {
        OccurrenceNode *current = globalHashtable.table[i];
        while (current != NULL)
        {
            OccurrenceNode *temp = current;
            current = current->next;
            free(temp);
        }
    }

    free(globalHashtable.table);

    // Dealloca la memoria utilizzata per l'array di occorrenze
    free(all_occurrences);
    free(local_occurrences);
}
// Funzione hash molto semplice (può essere migliorata)
unsigned int hashFunction(const char *word)
{
    unsigned int hash = 0;
    while (*word)
    {
        hash = (hash << 5) + *word++;
    }
    return hash;
}

// Funzione per aggiornare il conteggio delle occorrenze di una parola nella hashtable
void updateWordCount(const char *word)
{
    // Calcola l'indice della hashtable utilizzando una funzione hash
    unsigned int hashIndex = hashFunction(word) % globalHashtable.size;

    // Cerca la parola nella lista di trabocco corrispondente
    OccurrenceNode *current = globalHashtable.table[hashIndex];
    while (current != NULL)
    {
        if (strcmp(current->word, word) == 0)
        {
            // La parola esiste nella hashtable, incrementa il conteggio
            current->count++;
            return; // Esci dalla funzione dopo l'aggiornamento
        }
        current = current->next;
    }

    // La parola non esiste, aggiungila alla lista di trabocco
    OccurrenceNode *newNode = (OccurrenceNode *)malloc(sizeof(OccurrenceNode));
    newNode->count = 1; // Prima occorrenza
    strncpy(newNode->word, word, MAX_WORD_LENGTH - 1);
    newNode->word[MAX_WORD_LENGTH - 1] = '\0'; // Assicura che la stringa sia terminata correttamente
    newNode->next = globalHashtable.table[hashIndex];

    // Aggiorna il puntatore alla testa della lista di trabocco
    globalHashtable.table[hashIndex] = newNode;
}

// Funzione per generare l'array dall'hash table
OccurrenceNode *generateOccurrencesArray(int *numOccurrences)
{
    // Inizializza il conteggio totale delle parole
    *numOccurrences = 0;

    // Alloca dinamicamente l'array delle occorrenze
    OccurrenceNode *occurrencesArray = (OccurrenceNode *)malloc(globalHashtable.size * sizeof(OccurrenceNode));

    // Riempie l'array con le occorrenze dalla hashtable
    int currentIndex = 0;
    for (unsigned int i = 0; i < globalHashtable.size; ++i)
    {
        OccurrenceNode *current = globalHashtable.table[i];
        while (current != NULL)
        {
            occurrencesArray[currentIndex].count = current->count;
            strncpy(occurrencesArray[currentIndex].word, current->word, MAX_WORD_LENGTH - 1);
            occurrencesArray[currentIndex].word[MAX_WORD_LENGTH - 1] = '\0'; // Assicura che la stringa sia terminata correttamente

            currentIndex++;
            current = current->next;
        }
    }

    *numOccurrences = currentIndex; // Aggiorna il conteggio totale

    return occurrencesArray;
}

long get_total_file_size(DIR *dir, const char *input_dir)
{
    long total_size = 0;
    struct dirent *entry;
    struct stat info;

    while ((entry = readdir(dir)) != NULL)
    {
        // Ignora le directory "." e ".."
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
        {
            continue;
        }

        char filepath[1024];
        snprintf(filepath, sizeof(filepath), "%s/%s", input_dir, entry->d_name);

        if (stat(filepath, &info) == 0)
        {
            total_size += info.st_size;
        }
    }

    rewinddir(dir);
    return total_size;
}

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);

    int rank, num_processes;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_processes);
    // Dichiarazioni e inizializzazioni necessarie
    MPI_Request request_send_size[num_processes - 1];
    MPI_Request request_send_buffer[num_processes - 1];
    MPI_Request request_receive_size[num_processes - 1];
    MPI_Request request_receive_buffer[num_processes - 1];

    // Dichiarazione delle variabili per MPI
    int blockcounts[2] = {1, MAX_WORD_LENGTH};
    MPI_Aint offsets[2] = {offsetof(OccurrenceNode, count), offsetof(OccurrenceNode, word)};
    MPI_Datatype types[2] = {MPI_INT, MPI_CHAR};

    MPI_Type_create_struct(2, blockcounts, offsets, types, &MPI_OCCURRENCE_NODE);
    MPI_Type_commit(&MPI_OCCURRENCE_NODE);

    int total_occurrences = 0;
    int local_total_occurrences; // Sposta la dichiarazione fuori dai rami condizionali
    DIR *dir = opendir(INPUT_DIR);
    if (!dir)
    {
        perror("Errore nell'apertura della directory");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    long total_size = get_total_file_size(dir, INPUT_DIR);

    // Invia la dimensione totale del file a tutti i processi
    MPI_Bcast(&total_size, 1, MPI_LONG, MASTER_RANK, MPI_COMM_WORLD);

    if (rank == MASTER_RANK)
    {
        printf("MASTER: Dimensione totale dei file: %ld bytes\n", total_size);

        long offset = 0;
        long remainder = total_size % (num_processes - 1);
        long chunk_size = (total_size + remainder) / (num_processes - 1);

        long total_sent = 0;

        for (int i = 1; i < num_processes; i++)
        {
            // Alloca il buffer per il chunk
            char *chunk_buffer = (char *)malloc(chunk_size);

            // Leggi il chunk dai file
            long bytes_read = 0;
            while (bytes_read < chunk_size)
            {
                struct dirent *entry = readdir(dir);
                if (!entry)
                {
                    break; // Nessun altro file
                }

                // Ignora le directory "." e ".."
                if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
                {
                    continue;
                }

                char filepath[1024];
                snprintf(filepath, sizeof(filepath), "%s/%s", INPUT_DIR, entry->d_name);

                FILE *file = fopen(filepath, "rb");
                if (!file)
                {
                    perror("Errore nell'apertura del file");
                    MPI_Abort(MPI_COMM_WORLD, 1);
                }

                // Calcola la lunghezza del file
                fseek(file, 0L, SEEK_END);
                long current_file_size = ftell(file);
                fseek(file, 0L, SEEK_SET);

                // Leggi il contenuto del file nel chunk_buffer
                current_file_size = (current_file_size < chunk_size - bytes_read) ? current_file_size : chunk_size - bytes_read;
                fread(chunk_buffer + bytes_read, 1, current_file_size, file);

                // Aggiorna il numero di byte letti
                bytes_read += current_file_size;

                // Chiudi il file
                fclose(file);
            }

            total_sent += bytes_read;

            // Invia la dimensione del chunk al processo i
            // MPI_Send(&chunk_size, 1, MPI_LONG, i, 0, MPI_COMM_WORLD);

            // Invia il chunk al processo i
            // MPI_Send(chunk_buffer, chunk_size, MPI_CHAR, i, 0, MPI_COMM_WORLD);

            // Invio il chunk i-esimo
            MPI_Isend(&chunk_size, 1, MPI_LONG, i, 0, MPI_COMM_WORLD, &request_send_size[i - 1]);
            MPI_Isend(chunk_buffer, chunk_size, MPI_CHAR, i, 0, MPI_COMM_WORLD, &request_send_buffer[i - 1]);
            printf("MASTER: Invia la dimensione del chunk %d (%ld bytes) al processo %d\n", i, chunk_size, i);
            printf("MASTER: Invia il chunk %d al processo %d\n", i, i);
            // Attesa che tutte le operazioni di invio siano completate
            MPI_Waitall(num_processes - 1, request_send_size, MPI_STATUSES_IGNORE);
            MPI_Waitall(num_processes - 1, request_send_buffer, MPI_STATUSES_IGNORE);

            // Mostra il testo assegnato a ciascun processo
            printf("MASTER: Assegnato al processo %d: %.*s\n", i, (int)chunk_size, chunk_buffer);

            // Libera la memoria allocata per il buffer del chunk
            free(chunk_buffer);

            printf("MASTER: Inviato il chunk %d al processo %d\n", i, i);

            // Aggiorna l'offset per il prossimo chunk
            offset += chunk_size;
        }
        closedir(dir);

        if (total_sent != total_size)
        {
            printf("Errore: La somma dei byte inviati ai processi non corrisponde alla dimensione totale dei file.\n");
        }
        else
        {
            printf("La somma dei byte inviati ai processi corrisponde alla dimensione totale dei file.\n");
        }

        // Ricevi le occorrenze dai processi SLAVE
        for (int i = 1; i < num_processes; i++)
        {
            // Ricevi la partizione conteggiata
            MPI_Irecv(&local_total_occurrences, 1, MPI_INT, i, 0, MPI_COMM_WORLD, &request_receive_size[num_processes - 1]);
            MPI_Irecv(local_occurrences, local_total_occurrences, MPI_OCCURRENCE_NODE, i, 0, MPI_COMM_WORLD, &request_receive_buffer[num_processes - 1]);
            printf("MASTER: Ricevuto la dimensione del chunk %d (%ld bytes) dal processo %d\n", i, chunk_size, i);
            // Attesa che tutte le operazioni di ricezione siano completate
            MPI_Waitall(2, &request_receive_size[num_processes - 1], MPI_STATUSES_IGNORE);

            // Unisci le occorrenze ricevute al vettore totale
            all_occurrences = (OccurrenceNode *)realloc(all_occurrences, (total_occurrences + local_total_occurrences) * sizeof(OccurrenceNode));
            memcpy(all_occurrences + total_occurrences, local_occurrences, local_total_occurrences * sizeof(OccurrenceNode));

            total_occurrences += local_total_occurrences;
        }

        // Libera la memoria del buffer temporaneo
        free(all_occurrences);
    }
    else
    {
        // Ricevi la dimensione del chunk dal MASTER
        long chunk_size;
        MPI_Recv(&chunk_size, 1, MPI_LONG, MASTER_RANK, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        printf("Processo %d: Ricevuto dimensione del chunk: %ld\n", rank, chunk_size);
        // Alloca il buffer per il chunk
        char *recv_buffer = (char *)malloc(chunk_size);

        // Ricevi il chunk dal MASTER
        MPI_Recv(recv_buffer, chunk_size, MPI_CHAR, MASTER_RANK, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        // Invia la dimensione del chunk al MASTER
        MPI_Send(&chunk_size, 1, MPI_LONG, MASTER_RANK, 0, MPI_COMM_WORLD);
        printf("Processo %d: Invia dimensione del chunk: %ld\n", rank, chunk_size);

        // Invia il chunk al MASTER
        MPI_Send(recv_buffer, chunk_size, MPI_CHAR, MASTER_RANK, 0, MPI_COMM_WORLD);
        printf("Processo %d: Invia il chunk al MASTER\n", rank);

        // Ora il recv_buffer contiene il chunk ricevuto dal MASTER
        printf("Processo %d: Ha ricevuto il chunk: %.*s\n", rank, (int)chunk_size, recv_buffer);

        // Conta le parole nel chunk
        int num_words_to_count = chunk_size / MAX_WORD_LENGTH;
        for (int i = 0; i < num_words_to_count; ++i)
        {
            char word[MAX_WORD_LENGTH];
            strncpy(word, recv_buffer + i * MAX_WORD_LENGTH, MAX_WORD_LENGTH);
            word[MAX_WORD_LENGTH - 1] = '\0'; // Assicura che la stringa sia terminata correttamente
            updateWordCount(word);
        }

        // Libera la memoria allocata per il buffer del chunk
        free(recv_buffer);
    }

    MPI_Finalize();
    return 0;
}