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
int total_occurrences = 0;
int local_total_occurrences;
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
void updateWordCount(const char *word, int rank)
{
    printf("Processo %d: Updating word count for word: %s\n", rank, word);

    // Calcola l'indice della hashtable utilizzando una funzione hash
    if (globalHashtable.size == 0)
    {
        // La hashtable non è stata inizializzata correttamente
        printf("Processo %d: Hashtable non inizializzata correttamente.\n", rank);
        return;
    }

    unsigned int hashIndex = hashFunction(word) % globalHashtable.size;

     printf("Processo %d: Hash index: %u\n", rank, hashIndex);

    // Cerca la parola nella lista di trabocco corrispondente
    OccurrenceNode *current = globalHashtable.table[hashIndex];
    while (current != NULL)
    {
        if (strcmp(current->word, word) == 0)
        {
            // La parola esiste nella hashtable, incrementa il conteggio
            current->count++;
            printf("Processo %d: Word %s found. Count updated to %d\n", rank, word, current->count);
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

    // printf("Processo %d: New word %s added to hashtable. Count set to 1\n", rank, word);

    // Aggiorna la hashtable globale con i conteggi locali
    for (int i = 0; i < local_total_occurrences; ++i)
    {

        updateWordCount(local_occurrences[i].word, rank);
    }
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

    // Sposta la dichiarazione fuori dai rami condizionali
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
            char *chunk_buffer;
            MPI_Alloc_mem(chunk_size, MPI_INFO_NULL, &chunk_buffer);

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

                // Alloca il buffer per il chunk
                char *chunk_buffer;
                MPI_Alloc_mem(current_file_size, MPI_INFO_NULL, &chunk_buffer);

                // Leggi il contenuto del file nel chunk_buffer
                size_t bytes_read_now = fread(chunk_buffer, 1, current_file_size, file);
                if (bytes_read_now != current_file_size)
                {
                    // Gestisci l'errore o esegui il debug
                    printf("Errore nella lettura del file.\n");
                }

                // Aggiorna il numero di byte letti
                bytes_read += current_file_size;
                printf("MASTER: Bytes letti: %ld, Dimensione corrente del file: %ld\n", bytes_read, current_file_size);

                // Aggiungi una stampa di debug per visualizzare il contenuto del chunk letto
                printf("MASTER: Contenuto del chunk letto: %.*s\n", (int)current_file_size, chunk_buffer);

                // Stampa la dimensione effettiva del chunk prima di inviarlo
                printf("MASTER: Dimensione effettiva del chunk: %ld\n", bytes_read);

                // Chiudi il file
                fclose(file);
            }

            total_sent += bytes_read;

            // Invia la dimensione del chunk al processo i
            // MPI_Send(&chunk_size, 1, MPI_LONG, i, 0, MPI_COMM_WORLD);

            // Invia il chunk al processo i
            // MPI_Send(chunk_buffer, chunk_size, MPI_CHAR, i, 0, MPI_COMM_WORLD);

            // Subito prima di inviare il chunk al processo 1
            printf("MASTER: Dimensione effettiva del chunk inviato a processo %d: %ld\n", i, chunk_size);

            // Invio il chunk i-esimo
            MPI_Isend(&chunk_size, 1, MPI_LONG, i, 0, MPI_COMM_WORLD, &request_send_size[i - 1]);
            MPI_Isend(chunk_buffer, chunk_size, MPI_CHAR, i, 0, MPI_COMM_WORLD, &request_send_buffer[i - 1]);

            // Attesa che l'operazione di invio sia completata per il processo i
            MPI_Wait(&request_send_size[i - 1], MPI_STATUS_IGNORE);
            MPI_Wait(&request_send_buffer[i - 1], MPI_STATUS_IGNORE);

            // Mostra il testo assegnato a ciascun processo
            printf("MASTER: Assegnato al processo %d: %.*s\n", i, (int)chunk_size, chunk_buffer);

            printf("MASTER: Assegnato al processo %d: Chunk di dimensione %ld\n", i, chunk_size);

            printf("MASTER: Inviato il chunk %d al processo %d\n", i, i);

            // Aggiorna l'offset per il prossimo chunk
            offset += chunk_size;

            // Libera la memoria allocata con MPI_Alloc_mem
            MPI_Free_mem(chunk_buffer);
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
            MPI_Wait(&request_receive_size[i - 1], MPI_STATUS_IGNORE);
            MPI_Wait(&request_receive_buffer[i - 1], MPI_STATUS_IGNORE);

            // Unisci le occorrenze ricevute al vettore totale
            all_occurrences = (OccurrenceNode *)realloc(all_occurrences, (total_occurrences + local_total_occurrences) * sizeof(OccurrenceNode));
            memcpy(all_occurrences + total_occurrences, local_occurrences, local_total_occurrences * sizeof(OccurrenceNode));

            total_occurrences += local_total_occurrences;

            printf("MASTER: Ricevuto la dimensione del chunk %d (%ld bytes) dal processo %d\n", i, chunk_size, i);
        }

        // Aggiorna la hashtable globale con i conteggi totali
        for (int i = 0; i < total_occurrences; ++i)
        {
            updateWordCount(all_occurrences[i].word, MASTER_RANK);
        }

        // Libera la memoria del buffer temporaneo
        MPI_Free_mem(all_occurrences);
    }
    else
    {
        for (int i = 1; i < num_processes; i++)
        {
            // Ricevi la dimensione del chunk dal MASTER
            long chunk_size;
            MPI_Recv(&chunk_size, 1, MPI_LONG, MASTER_RANK, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            // Subito dopo la ricezione del chunk nel processo 1
            printf("Processo %d: Ha ricevuto il chunk. Dimensione: %ld bytes\n", i, chunk_size);

            // Alloca dinamicamente la memoria per il buffer del chunk
            char *recv_buffer;
            MPI_Alloc_mem(chunk_size, MPI_INFO_NULL, &recv_buffer);

            // Ricevi il chunk dal MASTER
            MPI_Recv(recv_buffer, chunk_size, MPI_CHAR, MASTER_RANK, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            printf("Processo %d: Ha ricevuto il chunk. Dimensione: %d bytes\n", rank, (int)chunk_size);

            // Inizializza la hashtable con una dimensione basata sulla dimensione del chunk
            initializeHashtable((unsigned int)chunk_size);

            // Suddividi il chunk in parole utilizzando spazi come delimitatori
            char *token = strtok(recv_buffer, " ");

            while (token != NULL)
            {
                // Aggiungi una stampa di debug per visualizzare le parole nel chunk
                printf("Processo %d: Parola nel chunk: %s\n", rank, token);

                // Aggiorna il conteggio delle parole
                updateWordCount(token, rank);

                // Continua con la prossima parola
                token = strtok(NULL, " ");
            }

            // Invia le occorrenze al MASTER
            MPI_Isend(&local_total_occurrences, 1, MPI_INT, MASTER_RANK, 0, MPI_COMM_WORLD, &request_send_size[rank - 1]);
            MPI_Isend(local_occurrences, local_total_occurrences, MPI_OCCURRENCE_NODE, MASTER_RANK, 0, MPI_COMM_WORLD, &request_send_buffer[rank - 1]);

            // Attesa che tutte le operazioni di invio siano completate
            MPI_Wait(&request_send_size[rank - 1], MPI_STATUS_IGNORE);
            MPI_Wait(&request_send_buffer[rank - 1], MPI_STATUS_IGNORE);

            // Libera la memoria allocata con MPI_Alloc_mem
            MPI_Free_mem(recv_buffer);
        }
    }

    MPI_Finalize();
    return 0;
}















































26_11

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>
#include <mpi.h>

#define MASTER_RANK 0
#define INPUT_DIR "./InputFile"
#define MAX_WORD_LENGTH 50

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

// Variabili globali per la memorizzazione delle occorrenze
OccurrenceNode *all_occurrences = NULL;
OccurrenceNode *local_occurrences;

// Variabili globali per i conteggi totali delle occorrenze
int total_occurrences = 0;
int local_total_occurrences;

// Funzione per inizializzare la hashtable con una dimensione specifica
void initializeHashtable(unsigned int size)
{
    globalHashtable.size = size;
    globalHashtable.table = (OccurrenceNode **)malloc(size * sizeof(OccurrenceNode *));

    for (unsigned int i = 0; i < size; ++i)
    {
        globalHashtable.table[i] = NULL;
    }
}

// Funzione per deallocare la memoria della hashtable
void freeHashtable()
{
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
void updateWordCount(const char *word, int rank)
{
    if (globalHashtable.size == 0)
    {
        printf("Processo %d: Hashtable non inizializzata correttamente.\n", rank);
        return;
    }

    unsigned int hashIndex = hashFunction(word) % globalHashtable.size;
    OccurrenceNode *current = globalHashtable.table[hashIndex];

    while (current != NULL)
    {
        if (strcmp(current->word, word) == 0)
        {
            current->count++;
            return;
        }
        current = current->next;
    }

    OccurrenceNode *newNode = (OccurrenceNode *)malloc(sizeof(OccurrenceNode));
    newNode->count = 1;
    strncpy(newNode->word, word, MAX_WORD_LENGTH - 1);
    newNode->word[MAX_WORD_LENGTH - 1] = '\0';
    newNode->next = globalHashtable.table[hashIndex];
    globalHashtable.table[hashIndex] = newNode;
}

// Funzione per generare l'array dalle occorrenze nella hashtable
OccurrenceNode *generateOccurrencesArray(int *numOccurrences)
{
    *numOccurrences = 0;
    OccurrenceNode *occurrencesArray = (OccurrenceNode *)malloc(globalHashtable.size * sizeof(OccurrenceNode));

    int currentIndex = 0;
    for (unsigned int i = 0; i < globalHashtable.size; ++i)
    {
        OccurrenceNode *current = globalHashtable.table[i];
        while (current != NULL)
        {
            occurrencesArray[currentIndex].count = current->count;
            strncpy(occurrencesArray[currentIndex].word, current->word, MAX_WORD_LENGTH - 1);
            occurrencesArray[currentIndex].word[MAX_WORD_LENGTH - 1] = '\0';

            currentIndex++;
            current = current->next;
        }
    }

    *numOccurrences = currentIndex;
    return occurrencesArray;
}

// Funzione per ottenere la dimensione totale dei file in una directory
long get_total_file_size(DIR *dir, const char *input_dir)
{
    long total_size = 0;
    struct dirent *entry;
    struct stat info;

    while ((entry = readdir(dir)) != NULL)
    {
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
    return total_size;
}

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);

    int rank, num_processes;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_processes);
    // Dichiarazione della struttura MPI_OCCURRENCE_NODE
    MPI_Datatype MPI_OCCURRENCE_NODE;

    // Dichiarazioni e inizializzazioni necessarie
    MPI_Request request_send_size[num_processes - 1];
    MPI_Request request_send_buffer[num_processes - 1];
    MPI_Request request_receive_size[num_processes - 1];
    MPI_Request request_receive_buffer[num_processes - 1];
    MPI_Request request_send_termination;
    // Dichiarazione dei blocchi e degli offset per MPI_Type_create_struct
    int blocklengths_occurrence_node[2] = {1, MAX_WORD_LENGTH};
    MPI_Aint displacements_occurrence_node[2] = {offsetof(OccurrenceNode, count), offsetof(OccurrenceNode, word)};
    MPI_Datatype types_occurrence_node[2] = {MPI_INT, MPI_CHAR};
    MPI_Type_create_struct(2, blocklengths_occurrence_node, displacements_occurrence_node, types_occurrence_node, &MPI_OCCURRENCE_NODE);
    MPI_Type_commit(&MPI_OCCURRENCE_NODE);
    int termination_message = 1;
    // Operazioni del processo master
    // ...

    DIR *dir = opendir(INPUT_DIR);
    if (!dir)
    {
        perror("Errore nell'apertura della directory");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    long total_size = get_total_file_size(dir, INPUT_DIR);
    MPI_Bcast(&total_size, 1, MPI_LONG, MASTER_RANK, MPI_COMM_WORLD);

   if (rank == MASTER_RANK)
{
    // Stampa la dimensione totale dei file
    printf("MASTER: Dimensione totale dei file: %ld bytes\n", total_size);

    // Calcola la dimensione dei chunk da inviare
    long total_sent = 0;
    long offset = 0;
    long remainder = total_size % (num_processes - 1);
    long chunk_size = (total_size - remainder) / (num_processes - 1);
    size_t bytes_read_now;

// Ciclo per inviare i chunk ai processi slave
for (int i = 1; i < num_processes; i++)
{
    char *chunk_buffer;

    // Allocazione di memoria per il chunk
    MPI_Alloc_mem(chunk_size, MPI_INFO_NULL, &chunk_buffer);
    printf("MASTER: Processo %d - Memoria allocata per il chunk.\n", rank);

    // Verifica se l'allocazione di memoria è riuscita
    if (chunk_buffer == NULL)
    {
        printf("MASTER: Errore nell'allocazione di memoria per il chunk.\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    // Inizializza il buffer del chunk a zero
    memset(chunk_buffer, 0, chunk_size);

    long bytes_read = 0;

    // Ciclo per leggere i file nella directory di input
    while (bytes_read < chunk_size)
        {
            struct dirent *entry = readdir(dir);
            if (!entry)
            {
                break;
            }

            // Ignora le voci "." e ".."
            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
            {
                continue;
            }

            char filepath[1024];
            snprintf(filepath, sizeof(filepath), "%s/%s", INPUT_DIR, entry->d_name);

            // Apertura del file in modalità binaria
            FILE *file = fopen(filepath, "rb");
            if (!file)
            {
                perror("MASTER: Errore nell'apertura del file");
                MPI_Abort(MPI_COMM_WORLD, 1);
            }

            // Sposta il puntatore alla fine del file per ottenere la dimensione corrente del file
            fseek(file, 0L, SEEK_END);
            long current_file_size = ftell(file);

            // Legge i byte dal file nel buffer del chunk
            bytes_read_now = fread(chunk_buffer + bytes_read, 1, current_file_size, file);
            if (bytes_read_now != current_file_size)
            {
                printf("MASTER: Errore nella lettura del file.\n");
            }

            bytes_read += bytes_read_now;

            // Stampa informazioni sul file letto
            printf("MASTER: Bytes letti: %ld, Dimensione corrente del file: %ld\n", bytes_read, current_file_size);
            printf("MASTER: Contenuto del chunk letto: %.*s\n", (int)current_file_size, chunk_buffer);
            printf("MASTER: Parola terminata al byte: %ld\n", bytes_read);

            // Trova la dimensione effettiva del chunk
            while (bytes_read < current_file_size && chunk_buffer[bytes_read - 1] != ' ')
            {
                bytes_read++;
            }

            printf("MASTER: Dimensione effettiva del chunk: %ld\n", bytes_read);

            fclose(file);
            rewinddir(dir);
        }

        // Calcola la dimensione effettiva del chunk da inviare a ciascun processo slave
        long current_chunk_size = chunk_size;
        if (i == num_processes - 1 && remainder > 0)
        {
            current_chunk_size += remainder;
        }

        // Stampa informazioni sul chunk e invia il chunk ai processi slave
        printf("MASTER: Processo %d - Dimensione effettiva del chunk inviato a processo %d: %ld\n", rank, i, chunk_size);
        MPI_Isend(&current_chunk_size, 1, MPI_LONG, i, 0, MPI_COMM_WORLD, &request_send_size[i - 1]);
        MPI_Isend(chunk_buffer + offset, current_chunk_size, MPI_CHAR, i, 0, MPI_COMM_WORLD, &request_send_buffer[i - 1]);
        printf("MASTER: Processo %d - Inviato il chunk %d al processo %d\n", rank, i, i);

        // Attendere che l'invio del chunk sia completato
        MPI_Waitall(num_processes - 1, &request_send_size[i - 1], MPI_STATUS_IGNORE);
        MPI_Waitall(num_processes - 1, &request_send_buffer[i - 1], MPI_STATUS_IGNORE);

        // Stampa ulteriori informazioni sul chunk inviato
        printf("MASTER: Processo %d - Assegnato al processo %d: %.*s\n", rank, i, (int)chunk_size, chunk_buffer);
        printf("MASTER: Processo %d - Assegnato al processo %d: Chunk di dimensione %ld\n", rank, i, chunk_size);
        printf("MASTER: Processo %d - Inviato il chunk %d al processo %d\n", rank, i, i);

        // Aggiorna l'offset e la dimensione totale inviata
        offset += bytes_read_now;
        total_sent += current_chunk_size;

        // Libera la memoria allocata per il buffer del chunk
        MPI_Free_mem(chunk_buffer);
    }

    // Chiudi la directory di input
    closedir(dir);

    // Verifica se la somma dei byte inviati ai processi corrisponde alla dimensione totale dei file
    if (total_sent != total_size)
    {
        printf("MASTER: Errore: La somma dei byte inviati ai processi non corrisponde alla dimensione totale dei file.\n");
    }
    else
    {
        printf("MASTER: La somma dei byte inviati ai processi corrisponde alla dimensione totale dei file.\n");
    }

    // Ricevi le conferme dai processi slave
    for (int i = 1; i < num_processes; i++)
    {
        MPI_Status status;
        MPI_Recv(&termination_message, 1, MPI_INT, i, 0, MPI_COMM_WORLD, &status);
        printf("MASTER: Processo %d - Ricevuto la conferma di terminazione dal processo %d\n", rank, i);
    }
    // continua
    }

    else
    {
        for (int i = 1; i < num_processes; i++)
        {
            long current_chunk_size;
            MPI_Recv(&current_chunk_size, 1, MPI_LONG, MASTER_RANK, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            char *recv_buffer;
            MPI_Alloc_mem(current_chunk_size, MPI_INFO_NULL, &recv_buffer);

            MPI_Recv(recv_buffer, current_chunk_size, MPI_CHAR, MASTER_RANK, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            printf("Processo %d: Ha ricevuto il chunk. Dimensione: %ld bytes\n", rank, current_chunk_size);
            if (recv_buffer == NULL)
{
    printf("Processo %d: Errore nell'allocazione di memoria per il chunk.\n", rank);
    MPI_Abort(MPI_COMM_WORLD, 1);
}

            MPI_Isend(&termination_message, 1, MPI_INT, MASTER_RANK, 0, MPI_COMM_WORLD, &request_send_termination);

            initializeHashtable((unsigned int)current_chunk_size);

            char *token = strtok(recv_buffer, " ");

            while (token != NULL)
            {
                printf("Processo %d: Parola nel chunk: %s\n", rank, token);
                updateWordCount(token, rank);
                printf("Processo %d: Parola %s aggiornata correttamente.\n", rank, token);
                token = strtok(NULL, " ");
            }

            MPI_Isend(&local_total_occurrences, 1, MPI_INT, MASTER_RANK, 0, MPI_COMM_WORLD, &request_send_size[rank - 1]);
            MPI_Isend(local_occurrences, local_total_occurrences, MPI_OCCURRENCE_NODE, MASTER_RANK, 0, MPI_COMM_WORLD, &request_send_buffer[rank - 1]);

            MPI_Waitall(num_processes - 1, &request_send_size[rank - 1], MPI_STATUS_IGNORE);
            MPI_Waitall(num_processes - 1, &request_send_buffer[rank - 1], MPI_STATUS_IGNORE);

            MPI_Free_mem(recv_buffer);
        }
    }
    
        MPI_Type_free(&MPI_OCCURRENCE_NODE);
        MPI_Finalize();
        return 0;
}